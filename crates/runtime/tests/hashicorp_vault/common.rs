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

//! Container helpers for the `hashicorp_vault` integration tests.
//!
//! These spin up a single-node Vault dev server (KV v2 mounted at the
//! default `secret/` path) and provide thin REST helpers for writing a
//! KV value and configuring `AppRole` auth, mirroring the manual flow we
//! used while bringing the secret store up.

use std::time::Duration;

use bollard::secret::HealthConfig;
use serde_json::{Value, json};
use tracing::instrument;

use crate::docker::{ContainerRunnerBuilder, RunningContainer};

pub const VAULT_ROOT_TOKEN: &str = "spice-vault-it-root";
const VAULT_DOCKER_CONTAINER: &str = "runtime-integration-test-vault";
/// Pinned to a recent Vault Enterprise/OSS image; using a tagged version
/// keeps the test reproducible if upstream bumps the `latest` tag.
const VAULT_IMAGE: &str = "hashicorp/vault:1.18";
const VAULT_CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(3);
const VAULT_SECRET_MOUNT_READY_TIMEOUT: Duration = Duration::from_secs(30);

#[instrument]
pub async fn start_vault_docker_container(
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{VAULT_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(VAULT_IMAGE.to_string())
        .add_port_binding(8200, port)
        .add_env_var("VAULT_DEV_ROOT_TOKEN_ID", VAULT_ROOT_TOKEN)
        .add_env_var("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
        .healthcheck(HealthConfig {
            // Use Vault's own status command so we wait for the dev
            // server to be unsealed and ready, not just for the TCP
            // socket to be open.
            test: Some(vec![
                "CMD-SHELL".to_string(),
                format!(
                    "VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN={VAULT_ROOT_TOKEN} \
                     vault status >/dev/null 2>&1"
                ),
            ]),
            interval: Some(250_000_000), // 250ms
            timeout: Some(500_000_000),  // 500ms
            retries: Some(20),
            start_period: Some(500_000_000), // 500ms
            start_interval: None,
        })
        .build()?
        .run(Some(VAULT_CONTAINER_START_TIMEOUT))
        .await?;

    wait_for_secret_mount(port).await?;
    Ok(running_container)
}

async fn wait_for_secret_mount(port: u16) -> Result<(), anyhow::Error> {
    let url = format!("{}/v1/sys/mounts/secret/tune", vault_address(port));
    let client = client()?;
    let start_time = std::time::Instant::now();
    let mut last_error = None;

    while start_time.elapsed() <= VAULT_SECRET_MOUNT_READY_TIMEOUT {
        match client
            .get(&url)
            .header("X-Vault-Token", VAULT_ROOT_TOKEN)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => last_error = Some(response.status().to_string()),
            Err(error) => last_error = Some(error.to_string()),
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(anyhow::anyhow!(
        "Vault container started but secret mount was not ready within {}s. Last error: {}",
        VAULT_SECRET_MOUNT_READY_TIMEOUT.as_secs(),
        last_error.unwrap_or_else(|| "none".to_string())
    ))
}

fn vault_address(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

fn client() -> Result<reqwest::Client, anyhow::Error> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?)
}

/// Write a KV v2 secret at `secret/<path>`. The `data` map becomes the
/// `data.data` payload — i.e. the same shape `vault kv put` produces.
pub async fn write_kv_v2_secret(
    port: u16,
    token: &str,
    path: &str,
    data: serde_json::Map<String, Value>,
) -> Result<(), anyhow::Error> {
    let url = format!("{}/v1/secret/data/{path}", vault_address(port));
    let resp = client()?
        .post(&url)
        .header("X-Vault-Token", token)
        .json(&json!({ "data": data }))
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("vault kv put {url} failed: {status}: {body}");
    }
    Ok(())
}

/// Approle credentials — the equivalent of the local `approle.env`
/// produced by the demo `setup.sh`.
pub struct AppRoleCreds {
    pub role_id: String,
    pub secret_id: String,
}

/// Enable the `AppRole` auth backend, install a read-only policy for the
/// demo path, and mint a `role_id` / `secret_id` pair.
pub async fn configure_approle(
    port: u16,
    token: &str,
    role_name: &str,
    policy_path: &str,
) -> Result<AppRoleCreds, anyhow::Error> {
    let addr = vault_address(port);
    let http = client()?;

    // Idempotent: enable approle if not already enabled. Vault returns
    // 400 with `path is already in use at approle/` on a second call.
    let enable_resp = http
        .post(format!("{addr}/v1/sys/auth/approle"))
        .header("X-Vault-Token", token)
        .json(&json!({ "type": "approle" }))
        .send()
        .await?;
    let enable_status = enable_resp.status();
    if !enable_status.is_success() && enable_status.as_u16() != 400 {
        let body = enable_resp.text().await.unwrap_or_default();
        anyhow::bail!("vault enable approle failed: {enable_status}: {body}");
    }

    // Policy granting read on the KV v2 data path.
    let policy_doc =
        format!("path \"secret/data/{policy_path}\" {{ capabilities = [\"read\"] }}\n");
    let policy_resp = http
        .put(format!("{addr}/v1/sys/policies/acl/spice-it"))
        .header("X-Vault-Token", token)
        .json(&json!({ "policy": policy_doc }))
        .send()
        .await?;
    if !policy_resp.status().is_success() {
        let status = policy_resp.status();
        let body = policy_resp.text().await.unwrap_or_default();
        anyhow::bail!("vault policy write failed: {status}: {body}");
    }

    // Create the role bound to that policy.
    let role_resp = http
        .post(format!("{addr}/v1/auth/approle/role/{role_name}"))
        .header("X-Vault-Token", token)
        .json(&json!({
            "token_policies": "spice-it",
            "token_ttl":     "1h",
            "token_max_ttl": "4h",
        }))
        .send()
        .await?;
    if !role_resp.status().is_success() {
        let status = role_resp.status();
        let body = role_resp.text().await.unwrap_or_default();
        anyhow::bail!("vault role create failed: {status}: {body}");
    }

    let role_id_resp = http
        .get(format!("{addr}/v1/auth/approle/role/{role_name}/role-id"))
        .header("X-Vault-Token", token)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let role_id = role_id_resp
        .pointer("/data/role_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing role_id in response"))?
        .to_string();

    let secret_id_resp = http
        .post(format!("{addr}/v1/auth/approle/role/{role_name}/secret-id"))
        .header("X-Vault-Token", token)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let secret_id = secret_id_resp
        .pointer("/data/secret_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing secret_id in response"))?
        .to_string();

    Ok(AppRoleCreds { role_id, secret_id })
}
