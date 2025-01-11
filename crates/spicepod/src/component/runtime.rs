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

use std::{collections::HashMap, error::Error, sync::Arc};

use super::is_default;
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const TASK_HISTORY_RETENTION_MINIMUM: u64 = 60; // 1 minute

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    #[serde(default, skip_serializing_if = "is_default")]
    pub results_cache: ResultsCache,

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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct ResultsCache {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub cache_max_size: Option<String>,
    pub item_ttl: Option<String>,
    pub eviction_policy: Option<String>,
}

const fn default_true() -> bool {
    true
}

impl Default for ResultsCache {
    fn default() -> Self {
        Self {
            enabled: true,
            cache_max_size: None,
            item_ttl: None,
            eviction_policy: None,
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

#[cfg(test)]
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
}
