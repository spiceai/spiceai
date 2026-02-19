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

use runtime_secrets::{ExposeSecret, Secrets};
use spice_cloud_client::CloudClient;
use test_framework::anyhow;

/// Set a single secret on a Spice Cloud app.
pub(crate) async fn set_secret(
    cloud: &CloudClient,
    app_id: i64,
    name: &str,
    value: &str,
) -> anyhow::Result<()> {
    cloud.set_secret(app_id, name, value).await?;
    Ok(())
}

/// Set all secrets from the spicepod YAML to the Spice Cloud app.
///
/// Extracts all secrets referenced in the spicepod YAML (regardless of store type), and sets them in SCP by using the local ENV variable of the same key name.
pub(crate) async fn set_spicepod_secrets(
    cloud: &CloudClient,
    app_id: i64,
    spicepod_yaml: &str,
) -> anyhow::Result<()> {
    let secret_refs = runtime_secrets::extract_secret_references(spicepod_yaml);

    eprintln!(
        "Found {} secret reference(s) in spicepod",
        secret_refs.len()
    );

    // Initialize Secrets instance to resolve secret values
    let mut secrets = Secrets::new();
    secrets
        .load_from(&[]) // Just environment variables
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load secrets from environment: {e}"))?;

    // For each secret reference, get its value and upload to Spice Cloud
    for (secret_key, store_name) in secret_refs {
        match secrets.get_secret(&secret_key).await {
            Ok(Some(secret_value)) => {
                println!("Setting secret: {secret_key} (from store: {store_name})");
                set_secret(cloud, app_id, &secret_key, secret_value.expose_secret()).await?;
            }
            Ok(None) => {
                eprintln!(
                    "Warning: Secret '{secret_key}' (referenced from store: {store_name}) not found, skipping"
                );
            }
            Err(e) => {
                eprintln!(
                    "Warning: Failed to get secret '{secret_key}' (from store: {store_name}): {e}, skipping"
                );
            }
        }
    }

    Ok(())
}
