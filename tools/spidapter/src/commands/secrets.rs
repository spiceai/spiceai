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
use spicepod::spec::SpicepodDefinition;
use url::Url;

const ENV_STORE_NAME: &str = "env";
const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const AWS_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";
const AWS_REGION: &str = "AWS_REGION";
const AWS_DEFAULT_REGION: &str = "AWS_DEFAULT_REGION";

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
    let mut secret_refs = runtime_secrets::extract_secret_references(spicepod_yaml);
    include_runtime_s3_aws_secrets(spicepod_yaml, &mut secret_refs);

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
                eprintln!("Setting secret: {secret_key} (from store: {store_name})");
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

fn include_runtime_s3_aws_secrets(
    spicepod_yaml: &str,
    secret_refs: &mut std::collections::HashMap<String, String>,
) {
    if !runtime_uses_s3_object_store(spicepod_yaml) {
        return;
    }

    for required_secret in [AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY] {
        secret_refs
            .entry(required_secret.to_string())
            .or_insert_with(|| ENV_STORE_NAME.to_string());
    }

    for optional_env in [AWS_SESSION_TOKEN, AWS_REGION, AWS_DEFAULT_REGION] {
        if !secret_refs.contains_key(optional_env) && std::env::var_os(optional_env).is_some() {
            secret_refs.insert(optional_env.to_string(), ENV_STORE_NAME.to_string());
        }
    }
}

fn runtime_uses_s3_object_store(spicepod_yaml: &str) -> bool {
    let spicepod: SpicepodDefinition = match yaml::from_str(spicepod_yaml) {
        Ok(spicepod) => spicepod,
        Err(_) => return false,
    };

    let scheduler_uses_s3 = spicepod
        .runtime
        .scheduler
        .as_ref()
        .is_some_and(|scheduler| url_uses_s3(scheduler.state_location.trim()));
    let shuffle_uses_s3 = spicepod
        .runtime
        .params
        .get("shuffle_location")
        .is_some_and(|loc| url_uses_s3(loc.trim()));

    scheduler_uses_s3 || shuffle_uses_s3
}

fn url_uses_s3(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }

    Url::parse(value)
        .map(|url| url.scheme().eq_ignore_ascii_case("s3"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASIC_SPICEPOD_YAML: &str = "
name: test
version: v2
kind: Spicepod
";

    const SCHEDULER_SPICEPOD_YAML: &str = "
name: test
version: v2
kind: Spicepod
runtime:
  scheduler:
    state_location: s3://bucket/path
";

    const FILE_SCHEDULER_SPICEPOD_YAML: &str = "
name: test
version: v2
kind: Spicepod
runtime:
    scheduler:
        state_location: file:///tmp/state
";

    const SHUFFLE_SPICEPOD_YAML: &str = "
name: test
version: v2
kind: Spicepod
runtime:
  params:
    shuffle_location: s3://bucket/shuffle
";

    #[test]
    fn includes_required_aws_secrets_when_scheduler_state_location_is_set() {
        let mut secret_refs = std::collections::HashMap::new();

        include_runtime_s3_aws_secrets(SCHEDULER_SPICEPOD_YAML, &mut secret_refs);

        assert_eq!(
            secret_refs.get(AWS_ACCESS_KEY_ID),
            Some(&ENV_STORE_NAME.to_string())
        );
        assert_eq!(
            secret_refs.get(AWS_SECRET_ACCESS_KEY),
            Some(&ENV_STORE_NAME.to_string())
        );
    }

    #[test]
    fn includes_required_aws_secrets_when_shuffle_location_is_s3() {
        let mut secret_refs = std::collections::HashMap::new();

        include_runtime_s3_aws_secrets(SHUFFLE_SPICEPOD_YAML, &mut secret_refs);

        assert_eq!(
            secret_refs.get(AWS_ACCESS_KEY_ID),
            Some(&ENV_STORE_NAME.to_string())
        );
        assert_eq!(
            secret_refs.get(AWS_SECRET_ACCESS_KEY),
            Some(&ENV_STORE_NAME.to_string())
        );
    }

    #[test]
    fn does_not_add_aws_secrets_when_runtime_s3_storage_is_absent() {
        let mut secret_refs = std::collections::HashMap::new();

        include_runtime_s3_aws_secrets(BASIC_SPICEPOD_YAML, &mut secret_refs);

        assert!(secret_refs.is_empty());
    }

    #[test]
    fn does_not_add_aws_secrets_for_non_s3_scheduler_state_location() {
        let mut secret_refs = std::collections::HashMap::new();

        include_runtime_s3_aws_secrets(FILE_SCHEDULER_SPICEPOD_YAML, &mut secret_refs);

        assert!(secret_refs.is_empty());
    }

    #[test]
    fn keeps_existing_store_mapping_for_required_secrets() {
        let mut secret_refs = std::collections::HashMap::from([(
            AWS_ACCESS_KEY_ID.to_string(),
            "custom-store".to_string(),
        )]);

        include_runtime_s3_aws_secrets(SCHEDULER_SPICEPOD_YAML, &mut secret_refs);

        assert_eq!(
            secret_refs.get(AWS_ACCESS_KEY_ID),
            Some(&"custom-store".to_string())
        );
        assert_eq!(
            secret_refs.get(AWS_SECRET_ACCESS_KEY),
            Some(&ENV_STORE_NAME.to_string())
        );
    }
}
