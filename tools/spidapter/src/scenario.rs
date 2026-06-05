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

use clap::ValueEnum;
use spice_cloud_client::types::UpdateChannel;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpiceCompute {
    Scp,
    Local,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AccelerationEngine {
    Cayenne,
    Duckdb,
}

/// Resource allocation for the SCP app (scheduler) and executor pods.
#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct ScpResources {
    pub app_memory: Option<String>,
    pub app_cpu: Option<String>,
    pub app_cpu_request: Option<String>,
    pub app_memory_request: Option<String>,
    pub app_replicas: Option<i32>,
    pub app_storage_size_gb: Option<f64>,
    pub executor_replicas: Option<i32>,
    pub executor_memory: Option<String>,
    pub executor_cpu: Option<String>,
    pub executor_cpu_request: Option<String>,
    pub executor_memory_request: Option<String>,
    pub executor_storage_size_gb: Option<f64>,
    pub ephemeral_storage_gb: Option<String>,
}

/// SCP-specific provisioning options nested under `compute: scp:`.
#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct ScpConfig {
    /// Runtime image release channel (e.g. `nightly`, `stable`).
    pub channel: Option<UpdateChannel>,
    /// Custom container image tag override.
    pub image_tag: Option<String>,
    /// Override Flight SQL endpoint URL.
    pub flight_url: Option<String>,
    /// S3 URL prefix for the spiced scheduler state location.
    pub scheduler_state_location: Option<String>,
    /// Organization tag to apply to the created app.
    pub organization_tag: Option<String>,
    /// Query memory limit (e.g. `150Gi`).
    pub query_memory_limit: Option<String>,
    /// Pod resource allocations.
    #[serde(default)]
    pub resources: ScpResources,
}

/// Where Spice runs — plain string `local` or structured `scp: { ... }`.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComputeConfig {
    Local,
    Scp(ScpConfig),
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct ScenarioConfig {
    pub compute: Option<ComputeConfig>,
    pub acceleration: Option<AccelerationEngine>,
    pub source: SourceConfig,
}

/// Cayenne-specific configuration nested under `source: direct: cayenne:`.
#[derive(Debug, Default, serde::Deserialize)]
#[allow(dead_code)]
pub(crate) struct CayenneConfig {
    pub aws_region: Option<String>,
    pub data_dir: Option<String>,
    pub metadata_dir: Option<String>,
    pub iceberg_region: Option<String>,
    pub iceberg_catalog_from: Option<String>,
}

/// Configuration for the direct-ingest source (no external CDC system).
#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct DirectConfig {
    pub cayenne: Option<CayenneConfig>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceConfig {
    Direct(DirectConfig),
    PostgresWal(PgEndpoint),
    PostgresDebezium(PgEndpoint),
    // Explicit rename: serde snake_case would produce `dynamo_db_streams`, not `dynamodb_streams`
    #[serde(rename = "dynamodb_streams")]
    DynamoDbStreams(DynamoDbConfig),
    MongodbStreams(MongoEndpoint),
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PgEndpoint {
    Connect(PgConnectConfig),
    Provision(ProvisionConfig),
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MongoEndpoint {
    Connect(MongoConnectConfig),
    Provision(ProvisionConfig),
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct ProvisionConfig {
    pub ec2: Ec2Spec,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct Ec2Spec {
    pub subnet_id: String,
    pub security_group_id: String,
    pub ami_id: String,
    pub instance_type: String,
    pub disk_size_gb: i32,
    pub associate_public_ip: bool,
    /// Empty string means not set; callers check `is_empty()`.
    #[serde(default)]
    pub iam_instance_profile: String,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct PgConnectConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct MongoConnectConfig {
    pub uri: String,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct DynamoDbConfig {
    pub region: String,
}

/// Load a scenario by name.
///
/// Resolution order:
/// 1. If `base_path` is set, try `<base_path>/<name>.yaml` from the filesystem.
/// 2. Fall back to the built-in scenarios embedded at compile time.
///
/// Env-var substitution (`${VAR}` / `${VAR:-default}`) is applied before parsing.
pub(crate) fn load_scenario(name: &str, base_path: Option<&str>) -> anyhow::Result<ScenarioConfig> {
    let from_fs = base_path.map(|base| {
        let path = std::path::Path::new(base).join(format!("{name}.yaml"));
        (path.exists(), path)
    });

    let raw_owned;
    let raw: &str = if let Some((true, ref path)) = from_fs {
        raw_owned = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read scenario file '{}': {e}", path.display()))?;
        eprintln!("[stdio] Loading scenario '{name}' from {}", path.display());
        &raw_owned
    } else {
        match name {
            "direct-ingest" => include_str!("../scenarios/direct-ingest.yaml"),
            "postgres-wal" => include_str!("../scenarios/postgres-wal.yaml"),
            "postgres-debezium" => include_str!("../scenarios/postgres-debezium.yaml"),
            "dynamodb-streams" => include_str!("../scenarios/dynamodb-streams.yaml"),
            "mongodb-streams" => include_str!("../scenarios/mongodb-streams.yaml"),
            other => anyhow::bail!(
                "Unknown scenario '{other}'. Built-in scenarios: \
                 direct-ingest, postgres-wal, postgres-debezium, dynamodb-streams, mongodb-streams"
            ),
        }
    };
    let substituted = envsubst(raw)
        .map_err(|e| anyhow::anyhow!("envsubst failed for scenario '{name}': {e}"))?;
    yaml::from_str(&substituted)
        .map_err(|e| anyhow::anyhow!("Failed to parse scenario '{name}': {e}"))
}

/// Resolve `${VAR}` and `${VAR:-default}` references in `input` using the process environment.
///
/// - `${VAR}` with no default and `VAR` unset → returns an error.
/// - `${VAR:-}` → empty string (callers check `is_empty()` to treat as absent).
pub(crate) fn envsubst(input: &str) -> anyhow::Result<String> {
    // Pattern: ${VAR} or ${VAR:-default}
    // Group 1: variable name (A-Z, 0-9, _)
    // Group 2: optional default value (everything after :-)
    let re = regex::Regex::new(r"\$\{([A-Z_][A-Z0-9_]*)(?::-(.*?))?\}").expect("valid regex");

    let mut result = String::with_capacity(input.len());
    let mut last = 0usize;

    for cap in re.captures_iter(input) {
        let full_match = cap.get(0).expect("full match");
        result.push_str(&input[last..full_match.start()]);

        let var_name = cap.get(1).expect("group 1").as_str();
        let default_val = cap.get(2).map(|m| m.as_str());

        let value = match std::env::var(var_name) {
            Ok(v) => v,
            Err(_) => match default_val {
                Some(d) => d.to_string(),
                None => anyhow::bail!(
                    "envsubst: environment variable '{var_name}' is not set and has no default"
                ),
            },
        };

        result.push_str(&value);
        last = full_match.end();
    }

    result.push_str(&input[last..]);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envsubst_plain_text_unchanged() {
        let result = envsubst("hello world").unwrap();
        assert_eq!(result, "hello world");
    }

    #[test]
    fn envsubst_resolves_set_var() {
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::set_var("SPIDAPTER_TEST_VAR_A", "resolved") };
        let result = envsubst("value: ${SPIDAPTER_TEST_VAR_A}").unwrap();
        unsafe { std::env::remove_var("SPIDAPTER_TEST_VAR_A") };
        assert_eq!(result, "value: resolved");
    }

    #[test]
    fn envsubst_uses_default_when_unset() {
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::remove_var("SPIDAPTER_TEST_VAR_UNSET") };
        let result = envsubst("value: ${SPIDAPTER_TEST_VAR_UNSET:-fallback}").unwrap();
        assert_eq!(result, "value: fallback");
    }

    #[test]
    fn envsubst_empty_default_when_unset() {
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::remove_var("SPIDAPTER_TEST_VAR_EMPTY") };
        let result = envsubst("iam: ${SPIDAPTER_TEST_VAR_EMPTY:-}").unwrap();
        assert_eq!(result, "iam: ");
    }

    #[test]
    fn envsubst_errors_on_unset_with_no_default() {
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::remove_var("SPIDAPTER_TEST_VAR_NODEFAULT") };
        let err = envsubst("value: ${SPIDAPTER_TEST_VAR_NODEFAULT}").unwrap_err();
        assert!(err.to_string().contains("SPIDAPTER_TEST_VAR_NODEFAULT"));
    }

    #[test]
    fn envsubst_set_var_overrides_default() {
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::set_var("SPIDAPTER_TEST_VAR_B", "override") };
        let result = envsubst("value: ${SPIDAPTER_TEST_VAR_B:-default}").unwrap();
        unsafe { std::env::remove_var("SPIDAPTER_TEST_VAR_B") };
        assert_eq!(result, "value: override");
    }
}
