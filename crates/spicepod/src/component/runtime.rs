/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};

use super::{
    caching::{Caching, ResultsCache},
    default_true, is_default, is_default_or_none,
};
use crate::metric::Metrics;
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const TASK_HISTORY_RETENTION_MINIMUM: u64 = 60; // 1 minute

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
#[serde(try_from = "RuntimeDeserializer")]
pub struct Runtime {
    #[serde(default, skip_serializing_if = "is_default_or_none")]
    pub results_cache: Option<ResultsCache>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub caching: Caching,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_load_parallelism: Option<usize>,

    /// If set, the runtime will configure all endpoints to use TLS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracing: Option<TracingConfig>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub telemetry: TelemetryConfig,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub params: HashMap<String, String>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub task_history: TaskHistory,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<Auth>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub cors: CorsConfig,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub flight: Option<Flight>,

    /// Configures how long the runtime waits for connections to be gracefully drained
    /// and components to shut down cleanly during runtime termination
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shutdown_timeout: Option<String>,

    /// Configures log level for the runtime. Can be overriden if flags or environment variables
    /// are set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_level: Option<OutputLevel>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<Query>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,
}

impl Runtime {
    pub fn shutdown_timeout(&self) -> Result<Option<Duration>, Box<dyn Error + Send + Sync>> {
        if let Some(timeout_str) = &self.shutdown_timeout {
            let duration = fundu::parse_duration(timeout_str)
                .map_err(|e| format!("Failed to parse 'shutdown_timeout': {e}"))?;

            if duration.as_secs() == 0 {
                return Err(
                    "'shutdown_timeout' must be a positive duration greater than 0 seconds".into(),
                );
            }

            Ok(Some(duration))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct TlsConfig {
    /// If set, the runtime will configure all endpoints to use TLS
    pub enabled: bool,

    /// A filesystem path to a file containing the PEM encoded certificate
    pub certificate_file: Option<String>,

    /// A PEM encoded certificate
    pub certificate: Option<String>,

    /// A filesystem path to a file containing the PEM encoded private key
    pub key_file: Option<String>,

    /// A PEM encoded private key
    pub key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct TracingConfig {
    pub zipkin_enabled: bool,
    pub zipkin_endpoint: Option<String>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum UserAgentCollection {
    #[default]
    Full,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct TelemetryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub user_agent_collection: UserAgentCollection,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            user_agent_collection: UserAgentCollection::default(),
            properties: HashMap::new(),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Flight {
    pub max_message_size: Option<String>,
}

impl Flight {
    pub fn max_message_size_bytes(&self) -> Result<Option<usize>, Box<dyn Error + Send + Sync>> {
        if let Some(size_str) = &self.max_message_size {
            let size_in_bytes = usize::try_from(
                byte_unit::Byte::parse_str(size_str, true)
                    .map_err(|e| {
                        format!("Failed to parse 'max_message_size' value '{size_str}': {e}")
                    })?
                    .as_u64(),
            )
            .unwrap_or_default();
            Ok(Some(size_in_bytes))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct TaskHistory {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_none")]
    pub captured_output: Arc<str>,
    #[serde(default = "default_retention_period")]
    pub retention_period: Arc<str>,
    #[serde(default = "default_retention_check_interval")]
    pub retention_check_interval: Arc<str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_sql_duration: Option<Arc<str>>,
}

fn default_none() -> Arc<str> {
    "none".into()
}

fn default_retention_period() -> Arc<str> {
    "8h".into()
}

fn default_retention_check_interval() -> Arc<str> {
    "15m".into()
}

impl Default for TaskHistory {
    fn default() -> Self {
        Self {
            enabled: true,
            captured_output: default_none(),
            retention_period: default_retention_period(),
            retention_check_interval: default_retention_check_interval(),
            min_sql_duration: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum TaskHistoryCapturedOutput {
    #[default]
    None,
    Truncated,
}

impl TaskHistory {
    pub fn get_captured_output(
        &self,
    ) -> Result<TaskHistoryCapturedOutput, Box<dyn Error + Send + Sync>> {
        if self.captured_output == "none".into() {
            return Ok(TaskHistoryCapturedOutput::None);
        } else if self.captured_output == "truncated".into() {
            return Ok(TaskHistoryCapturedOutput::Truncated);
        }

        Err(format!(
            r#"Expected "none" or "truncated" for "captured_output", but got: "{}""#,
            self.captured_output
        )
        .into())
    }

    fn retention_value_as_secs(
        value: &str,
        field: &str,
    ) -> Result<u64, Box<dyn Error + Send + Sync>> {
        let duration = fundu::parse_duration(value).map_err(|e| e.to_string())?;

        if duration.as_secs() < TASK_HISTORY_RETENTION_MINIMUM {
            return Err(format!(
                r#"Task history retention {field} must be at least {TASK_HISTORY_RETENTION_MINIMUM} seconds. To disable task history, set the property "enabled: false"."#,
            ).into());
        }

        Ok(duration.as_secs())
    }

    pub fn retention_period_as_secs(&self) -> Result<u64, Box<dyn Error + Send + Sync>> {
        Self::retention_value_as_secs(&self.retention_period, "period")
    }

    pub fn retention_check_interval_as_secs(&self) -> Result<u64, Box<dyn Error + Send + Sync>> {
        Self::retention_value_as_secs(&self.retention_check_interval, "check interval")
    }

    /// Parses the `min_sql_duration` field into milliseconds as f64. Returns `Ok(None)` if not set.
    ///
    /// # Errors
    ///
    /// Returns an error if the duration string cannot be parsed.
    pub fn min_sql_duration_as_millis(&self) -> Result<Option<f64>, Box<dyn Error + Send + Sync>> {
        let Some(min_sql_duration) = &self.min_sql_duration else {
            return Ok(None);
        };

        let duration =
            fundu::parse_duration(min_sql_duration.as_ref()).map_err(|e| e.to_string())?;

        Ok(Some(duration.as_secs_f64() * 1000.0))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Auth {
    #[serde(alias = "api-key")]
    pub api_key: Option<ApiKeyAuth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ApiKeyAuth {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub keys: Vec<ApiKey>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub enum ApiKey {
    ReadOnly { key: String },
    ReadWrite { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CorsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_allowed_origins")]
    pub allowed_origins: Vec<String>,
}

fn default_allowed_origins() -> Vec<String> {
    vec!["*".to_string()]
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            allowed_origins: default_allowed_origins(),
        }
    }
}

impl ApiKey {
    #[must_use]
    pub fn parse_str(input: &str) -> Self {
        if let Some((key, kind)) = input.rsplit_once(':') {
            match kind {
                "ro" => ApiKey::ReadOnly {
                    key: key.to_string(),
                },
                "rw" => ApiKey::ReadWrite {
                    key: key.to_string(),
                },
                _ => ApiKey::ReadOnly {
                    key: input.to_string(),
                },
            }
        } else {
            // Default to ReadOnly if no suffix is provided
            ApiKey::ReadOnly {
                key: input.to_string(),
            }
        }
    }
}

impl<'de> Deserialize<'de> for ApiKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;

        Ok(ApiKey::parse_str(&input))
    }
}

impl Serialize for ApiKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ApiKey::ReadOnly { key } => serializer.serialize_str(key),
            ApiKey::ReadWrite { key } => serializer.serialize_str(&format!("{key}:rw")),
        }
    }
}

impl PartialEq<str> for ApiKey {
    fn eq(&self, other: &str) -> bool {
        match self {
            ApiKey::ReadOnly { key } | ApiKey::ReadWrite { key } => key == other,
        }
    }
}

impl AsRef<str> for ApiKey {
    fn as_ref(&self) -> &str {
        match self {
            ApiKey::ReadOnly { key } | ApiKey::ReadWrite { key } => key.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum OutputLevel {
    #[default]
    Info,
    Verbose,
    VeryVerbose,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct Query {
    /// Specifies the runtime memory limit. When configured, will spill to disk
    /// for supported queries larger than memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit: Option<String>,

    /// Configures where the runtime will store temporary files needed for operations like
    /// spilling to disk for queries & accelerations that are larger than memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temp_directory: Option<String>,

    /// Specifies the compression codec used when spilling data to disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spill_compression: Option<SpillCompression>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SpillCompression {
    #[default]
    Zstd,
    Lz4Frame,
    Uncompressed,
}

/// Helper struct for deserializing Runtime with custom logic for handling `memory_limit`/`temp_directory` deprecation
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDeserializer {
    #[serde(default, skip_serializing_if = "is_default_or_none")]
    pub results_cache: Option<ResultsCache>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub caching: Caching,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_load_parallelism: Option<usize>,
    /// If set, the runtime will configure all endpoints to use TLS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracing: Option<TracingConfig>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub telemetry: TelemetryConfig,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub params: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub task_history: TaskHistory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<Auth>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub cors: CorsConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flight: Option<Flight>,
    /// Configures where the runtime will store temporary files needed for operations like
    /// spilling to disk for queries & accelerations that are larger than memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deprecated(since = "1.8.1", note = "Use `runtime.query.temp_directory` instead.")]
    pub temp_directory: Option<String>,
    /// Specifies the runtime memory limit. When configured, will spill to disk
    /// for supported queries larger than memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deprecated(since = "1.8.1", note = "Use `runtime.query.memory_limit` instead.")]
    pub memory_limit: Option<String>,
    /// Configures how long the runtime waits for connections to be gracefully drained
    /// and components to shut down cleanly during runtime termination
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shutdown_timeout: Option<String>,
    /// Configures log level for the runtime. Can be overriden if flags or environment variables
    /// are set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_level: Option<OutputLevel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<Query>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,
}

#[allow(deprecated)]
impl TryFrom<RuntimeDeserializer> for Runtime {
    type Error = String;

    fn try_from(deserializer: RuntimeDeserializer) -> Result<Self, Self::Error> {
        let mut query = deserializer.query.unwrap_or_default();

        query.memory_limit = match (
            deserializer.memory_limit.clone(),
            query.memory_limit.clone(),
        ) {
            // prefer runtime.query.memory_limit
            (_, Some(memory_limit)) => Some(memory_limit),
            (Some(memory_limit), None) => {
                tracing::warn!(
                    "`runtime.memory_limit` is deprecated, use `runtime.query.memory_limit` instead",
                );
                Some(memory_limit)
            }
            (None, None) => None,
        };

        query.temp_directory = match (
            deserializer.temp_directory.clone(),
            query.temp_directory.clone(),
        ) {
            // prefer runtime.query.temp_directory
            (_, Some(temp_directory)) => Some(temp_directory),
            (Some(temp_directory), None) => {
                tracing::warn!(
                    "`runtime.temp_directory` is deprecated, use `runtime.query.temp_directory` instead",
                );
                Some(temp_directory)
            }
            (None, None) => None,
        };

        Ok(Runtime {
            results_cache: deserializer.results_cache,
            caching: deserializer.caching,
            dataset_load_parallelism: deserializer.dataset_load_parallelism,
            tls: deserializer.tls,
            tracing: deserializer.tracing,
            telemetry: deserializer.telemetry,
            params: deserializer.params,
            task_history: deserializer.task_history,
            auth: deserializer.auth,
            cors: deserializer.cors,
            flight: deserializer.flight,
            shutdown_timeout: deserializer.shutdown_timeout,
            output_level: deserializer.output_level,
            query: if query == Query::default() {
                None
            } else {
                Some(query)
            },
            metrics: deserializer.metrics,
        })
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use serde_yaml;

    #[test]
    fn test_deserialize_api_keys() {
        let yaml = r"
        api_key:
            enabled: true
            keys:
                - api-key-1
                - api-key-2:ro
                - api-key-3:rw
        ";

        let parsed: Auth = serde_yaml::from_str(yaml).expect("Failed to parse Auth");

        let api_key = parsed.api_key.expect("api_key section exists");

        assert_eq!(
            api_key.keys[0],
            ApiKey::ReadOnly {
                key: "api-key-1".to_string()
            }
        );
        assert_eq!(
            api_key.keys[1],
            ApiKey::ReadOnly {
                key: "api-key-2".to_string()
            }
        );
        assert_eq!(
            api_key.keys[2],
            ApiKey::ReadWrite {
                key: "api-key-3".to_string()
            }
        );
    }

    #[test]
    fn test_deserialize_api_key_alternative_name() {
        let yaml = r"
        api-key:
            enabled: true
            keys:
                - api-key-1
        ";

        let parsed: Auth = serde_yaml::from_str(yaml).expect("Failed to parse Auth");

        let api_key = parsed.api_key.expect("api_key section exists");

        assert_eq!(
            api_key.keys[0],
            ApiKey::ReadOnly {
                key: "api-key-1".to_string()
            }
        );
    }

    #[test]
    fn test_memory_limit_migration() {
        // Test when only memory_limit is present
        let yaml = r"
            memory_limit: 100MiB
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(
            runtime.query,
            Some(Query {
                spill_compression: None,
                temp_directory: None,
                memory_limit: Some("100MiB".to_string())
            })
        );

        // Test when only query.memory_limit is present
        let yaml = r"
            query:
                memory_limit: 200MiB
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(
            runtime.query,
            Some(Query {
                spill_compression: None,
                temp_directory: None,
                memory_limit: Some("200MiB".to_string())
            })
        );

        // Test when both are present
        let yaml = r"
            memory_limit: 100MiB
            query:
                memory_limit: 200MiB
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(
            runtime.query,
            Some(Query {
                spill_compression: None,
                temp_directory: None,
                memory_limit: Some("200MiB".to_string())
            })
        );

        // Test when neither is present
        let yaml = r"
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(runtime.query, None);
    }

    #[test]
    fn test_temp_directory_migration() {
        // Test when only temp_directory is present
        let yaml = r"
            temp_directory: '/foo'
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(
            runtime.query,
            Some(Query {
                spill_compression: None,
                temp_directory: Some("/foo".to_string()),
                memory_limit: None
            })
        );

        // Test when only query.temp_directory is present
        let yaml = r"
            query:
                temp_directory: '/bar'
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(
            runtime.query,
            Some(Query {
                spill_compression: None,
                temp_directory: Some("/bar".to_string()),
                memory_limit: None
            })
        );

        // Test when both are present
        let yaml = r"
            temp_directory: '/foo'
            query:
                temp_directory: '/bar'
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(
            runtime.query,
            Some(Query {
                spill_compression: None,
                temp_directory: Some("/bar".to_string()),
                memory_limit: None
            })
        );

        // Test when neither is present
        let yaml = r"
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(runtime.query, None);
    }

    #[test]
    fn test_task_history_min_duration() {
        // Test default (no min_sql_duration)
        let task_history = TaskHistory::default();
        assert_eq!(task_history.min_sql_duration, None);
        assert_eq!(
            task_history
                .min_sql_duration_as_millis()
                .expect("should parse successfully"),
            None
        );

        // Test with various duration formats
        let test_cases = vec![
            ("5ms", 5.0),
            ("100ms", 100.0),
            ("1s", 1000.0),
            ("2.5s", 2500.0),
            ("1m", 60_000.0),
            ("1h", 3_600_000.0),
        ];

        for (duration_str, expected_ms) in test_cases {
            let task_history = TaskHistory {
                enabled: true,
                captured_output: "none".into(),
                retention_period: "8h".into(),
                retention_check_interval: "15m".into(),
                min_sql_duration: Some(duration_str.into()),
            };

            let result = task_history
                .min_sql_duration_as_millis()
                .expect("should parse successfully");
            assert_eq!(
                result,
                Some(expected_ms),
                "Failed for duration: {duration_str}"
            );
        }

        // Test invalid duration
        let task_history = TaskHistory {
            enabled: true,
            captured_output: "none".into(),
            retention_period: "8h".into(),
            retention_check_interval: "15m".into(),
            min_sql_duration: Some("invalid".into()),
        };
        assert!(
            task_history.min_sql_duration_as_millis().is_err(),
            "should fail for invalid duration"
        );
    }

    #[test]
    fn test_task_history_yaml_parsing() {
        // Test with min_sql_duration
        let yaml = r"
            task_history:
                enabled: true
                captured_output: truncated
                retention_period: 8h
                retention_check_interval: 15m
                min_sql_duration: 10ms
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(runtime.task_history.min_sql_duration, Some("10ms".into()));
        assert_eq!(
            runtime
                .task_history
                .min_sql_duration_as_millis()
                .expect("should parse"),
            Some(10.0)
        );

        // Test without min_sql_duration (should use default None)
        let yaml = r"
            task_history:
                enabled: true
        ";
        let runtime: Runtime = serde_yaml::from_str(yaml).expect("Failed to parse Runtime");
        assert_eq!(runtime.task_history.min_sql_duration, None);
    }
}
