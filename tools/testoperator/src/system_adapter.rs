/*
Copyright 2026 The Spice.ai OSS Authors

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

//! System-adapter integration for testoperator.
//!
//! When `--system-adapter-stdio-cmd` or `--system-adapter-http-url` is set,
//! testoperator delegates SUT acquisition to an out-of-process JSON-RPC adapter
//! (the same protocol spicebench uses). The adapter's `setup()` response carries
//! a Flight SQL URL in `db_kwargs` and, for clusters that expose Spice's HTTP
//! query APIs, an additional `endpoints` entry under `spice.http.v1.queries`.
//!
//! Lifecycle:
//!   1. `acquire()` — spawn or connect to the adapter, call `setup`, build a
//!      [`SpicedInstance::External`] from the response, return a paired
//!      [`SystemAdapterSession`] for teardown.
//!   2. Run the test workload as usual (existing engine path).
//!   3. `SystemAdapterSession::teardown()` — call `teardown` on the adapter.
//!      Best-effort: errors are logged, not propagated, since the test result
//!      should already be reported by this point.

use std::collections::HashMap;

use test_framework::{anyhow, spiced::SpicedInstance};
use uuid::Uuid;

use crate::args::CommonArgs;

/// JSON-RPC client + per-run identifier, retained so teardown can be invoked
/// after the test completes. `teardown` consumes `self`, so the client lives
/// here directly without `Arc<Mutex<_>>` indirection.
pub struct SystemAdapterSession {
    client: system_adapter_protocol::Client,
    run_id: Uuid,
    transport: &'static str,
}

impl SystemAdapterSession {
    /// Call `teardown` on the adapter. Errors are logged but not returned, so
    /// callers can always run teardown in their cleanup path regardless of
    /// whether the test succeeded.
    pub async fn teardown(mut self) {
        match self.client.teardown(self.run_id, false).await {
            Ok(response) if response.ok => {
                println!(
                    "System adapter teardown ({transport}, run_id={run_id}): ok",
                    transport = self.transport,
                    run_id = self.run_id,
                );
            }
            Ok(response) => {
                // Transport succeeded but the adapter reported failure; surface
                // it as a warning rather than burying it in normal output.
                eprintln!(
                    "Warning: system adapter teardown reported failure \
                     (transport={}, run_id={}, ok={})",
                    self.transport, self.run_id, response.ok,
                );
            }
            Err(e) => {
                eprintln!(
                    "Warning: system adapter teardown failed (transport={}, run_id={}): {e}",
                    self.transport, self.run_id,
                );
            }
        }
    }
}

/// Acquire a SUT via the configured system adapter.
///
/// Spawns (or connects to) the adapter, sends a `setup` request including the
/// resolved spicepod path and any `--system-adapter-param` values in the
/// metadata map, and returns a paired [`SpicedInstance::External`] + session
/// handle. The caller is responsible for calling [`SystemAdapterSession::teardown`]
/// in its cleanup path.
///
/// # Errors
/// - The adapter command can't be spawned, or the HTTP endpoint is unreachable
/// - `setup` returns an error or an unexpected driver (anything other than
///   `flightsql`)
/// - The response is missing the required `uri` kwarg
pub async fn acquire(args: &CommonArgs) -> anyhow::Result<(SpicedInstance, SystemAdapterSession)> {
    let mut client = build_client(args)?;
    let run_id = Uuid::new_v4();
    let transport = client.transport_name();

    let metadata = build_setup_metadata(args)?;

    println!("System adapter setup ({transport}, run_id={run_id})");

    let response = client
        .setup(run_id, metadata, HashMap::new())
        .await
        .map_err(|e| anyhow::anyhow!("system adapter setup failed: {e}"))?;

    // From this point on the adapter has provisioned resources. If anything
    // below fails (unsupported driver, missing uri, …) we still need to call
    // teardown so the SUT doesn't leak. Build the session first, then try to
    // interpret the response.
    let session = SystemAdapterSession {
        client,
        run_id,
        transport,
    };

    match interpret_setup_response(&response) {
        Ok((flight_url, api_key, http_base_url)) => {
            let instance = match http_base_url {
                Some(http_url) => SpicedInstance::external_with_http(flight_url, http_url),
                None => SpicedInstance::external(flight_url),
            };
            let instance = instance.with_api_key(api_key);
            Ok((instance, session))
        }
        Err(e) => {
            session.teardown().await;
            Err(e)
        }
    }
}

/// Validate the setup response and extract `(flight_url, api_key, http_base_url)`.
fn interpret_setup_response(
    response: &system_adapter_protocol::SetupResponse,
) -> anyhow::Result<(String, Option<String>, Option<String>)> {
    if !matches!(
        response.read_driver,
        system_adapter_protocol::AdbcDriver::Flightsql
    ) {
        anyhow::bail!(
            "system adapter returned unsupported driver `{driver}`; testoperator only \
             drives `flightsql` SUTs today",
            driver = response.read_driver,
        );
    }

    let flight_url = response
        .read_db_kwargs
        .get("uri")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "system adapter setup response missing required `uri` in read_db_kwargs \
                 for driver=flightsql"
            )
        })?
        .to_string();

    let http_base_url = http_base_url_from_endpoints(&response.endpoints)
        .or_else(|| derive_http_base_url(&flight_url));

    // Spice Cloud + spidapter return the cluster's API key in the standard
    // FlightSQL ADBC kwarg `password`. We stash it on the SpicedInstance so the
    // HTTP readiness probe and Flight SQL client both authenticate properly.
    let api_key = response
        .read_db_kwargs
        .get("password")
        .and_then(serde_json::Value::as_str)
        .filter(|s: &&str| !s.is_empty())
        .map(str::to_string);

    Ok((flight_url, api_key, http_base_url))
}

fn build_client(args: &CommonArgs) -> anyhow::Result<system_adapter_protocol::Client> {
    if let Some(cmd) = &args.system_adapter_stdio_cmd {
        let stdio_args = args
            .system_adapter_stdio_args
            .as_deref()
            .map(|s| s.split_whitespace().map(str::to_string).collect::<Vec<_>>())
            .unwrap_or_default();
        let env: HashMap<String, String> = args.system_adapter_env.iter().cloned().collect();
        return system_adapter_protocol::Client::stdio(cmd, stdio_args, env)
            .map_err(|e| anyhow::anyhow!("failed to start stdio system adapter `{cmd}`: {e}"));
    }

    if let Some(url) = &args.system_adapter_http_url {
        return Ok(system_adapter_protocol::Client::http(url));
    }

    anyhow::bail!(
        "no system adapter configured (expected --system-adapter-stdio-cmd or \
         --system-adapter-http-url)"
    )
}

fn build_setup_metadata(args: &CommonArgs) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    let mut metadata: HashMap<String, serde_json::Value> = HashMap::new();

    let spicepod_path = args
        .spicepod_path
        .canonicalize()
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to resolve spicepod path `{}` to an absolute path for the \
                 system adapter: {e}",
                args.spicepod_path.display()
            )
        })?
        .to_string_lossy()
        .into_owned();

    metadata.insert("spicepod_path".to_string(), spicepod_path.into());
    metadata.insert(
        "system_adapter_name".to_string(),
        args.system_adapter_name.clone().into(),
    );

    for (key, value) in &args.system_adapter_param {
        metadata.insert(key.clone(), value.clone().into());
    }

    Ok(metadata)
}

/// Pull an HTTP base URL out of the optional `spice.http.v1.queries` endpoint.
///
/// The endpoint's `url` kwarg is the full Ballista submit URL
/// (e.g. `http://scheduler:8090/v1/queries`); we want the base URL with the
/// `/v1/queries` path component stripped so callers can re-append the right
/// suffix (`/v1/ready`, `/v1/queries`, …). Parse as a URL so trailing slashes,
/// query strings, and any future API-version drift don't desync.
fn http_base_url_from_endpoints(
    endpoints: &HashMap<String, HashMap<String, serde_json::Value>>,
) -> Option<String> {
    let entry = endpoints.get("spice.http.v1.queries")?;
    let raw = entry.get("url")?.as_str()?;
    let mut url = url::Url::parse(raw).ok()?;
    // Drop everything past the origin — path/query/fragment — leaving just
    // `scheme://host[:port]`.
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    let s = url.as_str().trim_end_matches('/').to_string();
    Some(s)
}

/// Mirror of `SpicedInstance::external`'s URL inference, exposed here so we can
/// fall back to a derived HTTP base URL when the adapter doesn't return one.
fn derive_http_base_url(flight_url: &str) -> Option<String> {
    if flight_url.contains("flight.spiceai.io") {
        return Some("https://data.spiceai.io".to_string());
    }
    flight_url
        .rfind(':')
        .map(|last_colon| format!("{base}:8090", base = &flight_url[..last_colon]))
}
