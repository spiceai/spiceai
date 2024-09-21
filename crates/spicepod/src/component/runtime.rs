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

use std::error::Error;

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const TASK_HISTORY_RETENTION_MINIMUM: u64 = 60; // 1 minute

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Runtime {
    #[serde(default)]
    pub results_cache: ResultsCache,
    pub num_of_parallel_loading_at_start_up: Option<usize>,

    /// If set, the runtime will configure all endpoints to use TLS
    pub tls: Option<TlsConfig>,

    pub tracing: Option<TracingConfig>,

    pub telemetry: Option<TelemetryConfig>,

    #[serde(default)]
    pub task_history: TaskHistory,
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
    #[serde(default = "default_truncated")]
    pub captured_output: String,
    #[serde(default = "default_retention_period")]
    pub retention_period: String,
    #[serde(default = "default_retention_check_interval")]
    pub retention_check_interval: String,
}

fn default_truncated() -> String {
    "truncated".to_string()
}

fn default_retention_period() -> String {
    "1d".to_string()
}

fn default_retention_check_interval() -> String {
    "30m".to_string()
}

impl Default for TaskHistory {
    fn default() -> Self {
        Self {
            enabled: true,
            captured_output: "truncated".to_string(),
            retention_period: "1d".to_string(),
            retention_check_interval: "30m".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskHistoryCapturedOutput {
    None,
    Truncated,
}

impl TaskHistory {
    pub fn get_captured_output(
        &self,
    ) -> Result<TaskHistoryCapturedOutput, Box<dyn Error + Send + Sync>> {
        match self.captured_output.as_str() {
            "none" => Ok(TaskHistoryCapturedOutput::None),
            "truncated" => Ok(TaskHistoryCapturedOutput::Truncated),
            _ => Err(format!(
                r#"Expected "none" or "truncated" for captured_output, but got: "{}""#,
                self.captured_output
            )
            .into()),
        }
    }

    fn retention_value_as_secs(value: &str) -> Result<u64, Box<dyn Error + Send + Sync>> {
        let duration = humantime::parse_duration(value).map_err(|e| e.to_string())?;

        if duration.as_secs() < TASK_HISTORY_RETENTION_MINIMUM {
            return Err(format!(
                "Task history retention period must be at least {TASK_HISTORY_RETENTION_MINIMUM} seconds",
            ).into());
        }

        Ok(duration.as_secs())
    }

    pub fn retention_period_as_secs(&self) -> Result<u64, Box<dyn Error + Send + Sync>> {
        Self::retention_value_as_secs(&self.retention_period)
    }

    pub fn retention_check_interval_as_secs(&self) -> Result<u64, Box<dyn Error + Send + Sync>> {
        Self::retention_value_as_secs(&self.retention_check_interval)
    }
}
