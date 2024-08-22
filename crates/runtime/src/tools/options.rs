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
#![allow(clippy::missing_errors_doc)]

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::SpiceModelTool;

/// Options to specify which and how tools can be used by a specific LLM.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpiceToolsOptions {
    /// Automatically use all available builtin tools.
    Auto,

    /// Disable all tools.
    Disabled,

    /// Use only the tools specified in the list. Values correspond to [`SpiceModelTool::name`] for tools registered in the runtime.
    Specific(Vec<String>),
}

impl SpiceToolsOptions {
    // Check if spice tools can be used.
    #[must_use]
    pub fn can_use_tools(&self) -> bool {
        match self {
            SpiceToolsOptions::Auto => true,
            SpiceToolsOptions::Disabled => false,
            SpiceToolsOptions::Specific(t) => !t.is_empty(),
        }
    }

    /// Filter out a list of tools permitted by the  [`SpiceToolsOptions`].
    #[must_use]
    pub fn filter_tools(
        &self,
        tools: Vec<Box<dyn SpiceModelTool>>,
    ) -> Vec<Box<dyn SpiceModelTool>> {
        match self {
            SpiceToolsOptions::Auto => tools,
            SpiceToolsOptions::Disabled => vec![],
            SpiceToolsOptions::Specific(t) => tools
                .into_iter()
                .filter(|tool| t.iter().any(|s| s == tool.name()))
                .collect(),
        }
    }
}

impl FromStr for SpiceToolsOptions {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "auto" => Ok(SpiceToolsOptions::Auto),
            "disabled" => Ok(SpiceToolsOptions::Disabled),
            _ => Ok(SpiceToolsOptions::Specific(
                s.split(',')
                    .map(|item| item.trim().to_string())
                    .filter(|item| !item.is_empty())
                    .collect(),
            )),
        }
    }
}
