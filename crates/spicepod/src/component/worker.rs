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
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Worker {
    pub name: String,

    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_balance: Option<LoadBalanceParams>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
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
            load_balance: self.load_balance.clone(),
            cron: self.cron.clone(),
            sql: self.sql.clone(),
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct LoadBalanceParams {
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_router_configs"
    )]
    pub routing: Vec<RouterConfig>,
}

fn deserialize_router_configs<'de, D>(deserializer: D) -> Result<Vec<RouterConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    let configs = Vec::<RouterConfig>::deserialize(deserializer)?;

    // If there is at least one element, check they are all the same variant.
    let Some(first) = configs.first() else {
        return Err(de::Error::custom(
            "Worker requires at least one model specified in `.models`",
        ));
    };

    let strategy: RoutingStrategy = first.into();

    if configs.iter().any(|c| RoutingStrategy::from(c) != strategy) {
        return Err(de::Error::custom(
            "All `.models` must be the same format, but are not.",
        ));
    }

    Ok(configs)
}
