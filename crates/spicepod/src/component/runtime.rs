/*
Copyright 2024 The Spice.ai OSS Authors

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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const TASK_HISTORY_RETENTION_MINIMUM: u64 = 60; // 1 minute

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    #[serde(default)]
    pub results_cache: ResultsCache,
    pub dataset_load_parallelism: Option<usize>,

    /// If set, the runtime will configure all endpoints to use TLS
    pub tls: Option<TlsConfig>,

    pub tracing: Option<TracingConfig>,

    pub telemetry: Option<TelemetryConfig>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub params: HashMap<String, String>,

    #[serde(default)]
    pub task_history: TaskHistory,

    #[serde(default)]
    pub auth: Option<Auth>,

    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct TelemetryConfig {
    pub enabled: bool,
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
    #[serde(rename = "api-key")]
    pub api_key: Option<ApiKeyAuth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ApiKeyAuth {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub keys: Vec<String>,
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
