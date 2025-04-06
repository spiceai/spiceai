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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use crate::component::Nameable;

#[derive(Debug, Clone, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Metrics {
    pub metrics: Vec<Metric>,
}

impl Serialize for Metrics {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.metrics.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Metrics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let metrics = Vec::<Metric>::deserialize(deserializer)?;
        Ok(Metrics { metrics })
    }
}

impl Metrics {
    #[must_use]
    pub fn enabled_metrics(&self) -> Vec<String> {
        self.metrics
            .iter()
            .filter(|m| m.enabled)
            .map(|m| m.name.clone())
            .collect()
    }

    #[must_use]
    pub fn has_enabled_metrics(&self) -> bool {
        self.metrics.iter().any(|m| m.enabled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Metric {
    #[serde(default = "default_true")]
    pub enabled: bool,

    pub name: String,
}

impl Nameable for Metric {
    fn name(&self) -> &str {
        &self.name
    }
}

const fn default_true() -> bool {
    true
}
