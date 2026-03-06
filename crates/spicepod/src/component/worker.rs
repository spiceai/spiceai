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

use std::collections::HashMap;

use super::{Nameable, WithDependsOn};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Worker {
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, Value>,

    /// Action routing configuration for the worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<Routing>,

    /// Schedule and lifecycle triggers for the worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triggers: Option<TriggerConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct WorkerSpec {
    name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    params: HashMap<String, Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    routing: Option<Routing>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    triggers: Option<TriggerConfig>,
}

impl TryFrom<WorkerSpec> for Worker {
    type Error = String;

    fn try_from(spec: WorkerSpec) -> Result<Self, Self::Error> {
        let worker = Self {
            name: spec.name,
            description: spec.description,
            params: spec.params,
            routing: spec.routing,
            triggers: spec.triggers,
        };

        worker.validate()?;
        Ok(worker)
    }
}

impl<'de> Deserialize<'de> for Worker {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let spec = WorkerSpec::deserialize(deserializer)?;
        Self::try_from(spec).map_err(de::Error::custom)
    }
}

impl Nameable for Worker {
    fn name(&self) -> &str {
        &self.name
    }
}

impl WithDependsOn<Worker> for Worker {
    fn depends_on(&self, _depends_on: &[String]) -> Worker {
        Worker {
            name: self.name.clone(),
            description: self.description.clone(),
            params: self.params.clone(),
            routing: self.routing.clone(),
            triggers: self.triggers.clone(),
        }
    }
}

impl Worker {
    pub fn validate(&self) -> std::result::Result<(), String> {
        let Some(routing) = &self.routing else {
            return Err(format!(
                "Worker '{}' must set 'routing'",
                self.name
            ));
        };

        let configured_modes = usize::from(routing.prompt.is_some())
            + usize::from(routing.sql.is_some())
            + usize::from(routing.webhook.is_some());

        if configured_modes != 1 {
            return Err(format!(
                "Worker '{}' must set exactly one of 'routing.prompt', 'routing.sql', or 'routing.webhook'",
                self.name
            ));
        }

        let cron = self.triggers.as_ref().and_then(|triggers| triggers.cron.as_ref());
        let events = self
            .triggers
            .as_ref()
            .and_then(|triggers| triggers.event.as_ref());

        if !routing.models.is_empty() {
            validate_router_configs(&routing.models)?;
        }

        if routing.prompt.is_some() && routing.models.is_empty() {
            return Err(format!(
                "Worker '{}' must set 'routing.models' when 'routing.prompt' is configured",
                self.name
            ));
        }

        if !routing.models.is_empty() && routing.prompt.is_none() {
            return Err(format!(
                "Worker '{}' can only set 'routing.models' when 'routing.prompt' is configured",
                self.name
            ));
        }

        if let Some(triggers) = &self.triggers
            && triggers.cron.is_none()
            && triggers.event.is_none()
        {
            return Err(format!(
                "Worker '{}' must set at least one of 'triggers.cron' or 'triggers.event' when 'triggers' is configured",
                self.name
            ));
        }

        if routing.webhook.is_some() {
            if cron.is_some() {
                return Err(format!(
                    "Worker '{}' cannot set 'triggers.cron' for a webhook worker",
                    self.name
                ));
            }

            if !routing.models.is_empty() {
                return Err(format!(
                    "Worker '{}' cannot set 'routing.models' for a webhook worker",
                    self.name
                ));
            }
        }

        if routing.sql.is_some() {
            if !routing.models.is_empty() {
                return Err(format!(
                    "Worker '{}' cannot set 'routing.models' for a SQL worker",
                    self.name
                ));
            }

            if events.is_some() {
                return Err(format!(
                    "Worker '{}' cannot set 'triggers.event' for a SQL worker",
                    self.name
                ));
            }
        }

        if routing.prompt.is_some() && events.is_some() {
            return Err(format!(
                "Worker '{}' cannot set 'triggers.event' for a prompt worker",
                self.name
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Routing {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<String>,

    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_router_configs"
    )]
    pub models: Vec<RouterConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct TriggerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<EventFilters>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum Toggle {
    Enabled,
    Disabled,
}

impl Toggle {
    #[must_use]
    pub fn is_enabled(self) -> bool {
        matches!(self, Toggle::Enabled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct EventFilters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errors: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datasets: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub views: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub models: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_indexing: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_vectorization: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accelerations: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refreshes: Option<Toggle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crons: Option<Toggle>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum RoutingStrategy {
    #[default]
    Fallback,
    RoundRobin,
    Weighted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(untagged)]
pub enum RouterConfig {
    Fallback { from: String, order: u32 },
    Weighted { from: String, weight: u32 },
    RoundRobin { from: String }, // Must be last for deserialization.
}

impl RouterConfig {
    #[must_use]
    pub fn from(&self) -> String {
        match self {
            RouterConfig::Fallback { from, .. }
            | RouterConfig::RoundRobin { from }
            | RouterConfig::Weighted { from, .. } => from.clone(),
        }
    }
}

impl From<&RouterConfig> for RoutingStrategy {
    fn from(value: &RouterConfig) -> Self {
        match value {
            RouterConfig::Fallback { .. } => RoutingStrategy::Fallback,
            RouterConfig::RoundRobin { .. } => RoutingStrategy::RoundRobin,
            RouterConfig::Weighted { .. } => RoutingStrategy::Weighted,
        }
    }
}

/// Validates that all router configs use the same routing strategy.
pub fn validate_router_configs(configs: &[RouterConfig]) -> std::result::Result<(), String> {
    let Some(first) = configs.first() else {
        return Err("Worker requires at least one model specified in routing".to_string());
    };

    let strategy: RoutingStrategy = first.into();

    if configs.iter().any(|c| RoutingStrategy::from(c) != strategy) {
        return Err("All routing entries must use the same format".to_string());
    }

    Ok(())
}

fn deserialize_router_configs<'de, D>(deserializer: D) -> Result<Vec<RouterConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    let configs = Vec::<RouterConfig>::deserialize(deserializer)?;
    validate_router_configs(&configs).map_err(de::Error::custom)?;
    Ok(configs)
}

#[cfg(test)]
mod tests {
    use super::{RouterConfig, Worker, validate_router_configs};

    #[test]
    fn parses_valid_sql_worker() {
        let yaml = r#"
name: daily_aggregation
routing:
  sql: SELECT 1
triggers:
  cron: 0 0 * * *
"#;

        let worker: Worker = yaml::from_str(yaml).expect("worker should parse");
        assert_eq!(
            worker
                .routing
                .as_ref()
                .and_then(|routing| routing.sql.as_deref()),
            Some("SELECT 1")
        );
    }

    #[test]
    fn rejects_multiple_worker_modes() {
        let yaml = r#"
name: invalid_worker
routing:
  sql: SELECT 1
  webhook: https://example.com/hook
"#;

        let err = yaml::from_str::<Worker>(yaml).expect_err("worker should fail to parse");
        assert!(err
            .to_string()
            .contains("exactly one of 'routing.prompt', 'routing.sql', or 'routing.webhook'"));
    }

    #[test]
    fn rejects_events_without_webhook() {
        let yaml = r#"
name: invalid_worker
routing:
  sql: SELECT 1
triggers:
    event:
        success: enabled
"#;

        let err = yaml::from_str::<Worker>(yaml).expect_err("worker should fail to parse");
        assert!(err
            .to_string()
                        .contains("cannot set 'triggers.event' for a SQL worker"));
    }

    #[test]
    fn rejects_prompt_without_routing() {
        let yaml = r#"
name: invalid_worker
routing:
    prompt: hello
"#;

        let err = yaml::from_str::<Worker>(yaml).expect_err("worker should fail to parse");
        assert!(err
            .to_string()
            .contains("must set 'routing.models' when 'routing.prompt' is configured"));
    }

    #[test]
    fn rejects_mixed_routing_strategies() {
        let err = validate_router_configs(&[
            RouterConfig::Fallback {
                from: "model_a".to_string(),
                order: 1,
            },
            RouterConfig::Weighted {
                from: "model_b".to_string(),
                weight: 2,
            },
        ])
        .expect_err("routing validation should fail");

        assert!(err.contains("All routing entries must use the same format"));
    }

    #[test]
    fn rejects_empty_triggers() {
        let yaml = r#"
name: invalid_worker
routing:
  sql: SELECT 1
triggers: {}
"#;

        let err = yaml::from_str::<Worker>(yaml).expect_err("worker should fail to parse");
        assert!(err
            .to_string()
            .contains("must set at least one of 'triggers.cron' or 'triggers.event'"));
    }
}
