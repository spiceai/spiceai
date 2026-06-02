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
#![allow(clippy::missing_errors_doc)]

use std::str::FromStr;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// Options to specify which and how tools can be used by a specific LLM.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpiceToolsOptions {
    /// Automatically choose between direct configured tools and searchable discovery.
    Auto,

    /// Use built-in and Spicepod-configured tools directly.
    All,

    /// Use a searchable registry over built-in and Spicepod-configured tools.
    #[serde(rename = "search_registry")]
    SearchRegistry,

    /// Use builtin tools relevant for text-to-SQL.
    Nsql,

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
            SpiceToolsOptions::Auto
            | SpiceToolsOptions::All
            | SpiceToolsOptions::SearchRegistry
            | SpiceToolsOptions::Nsql => true,
            SpiceToolsOptions::Disabled => false,
            SpiceToolsOptions::Specific(t) => !t.is_empty(),
        }
    }

    #[must_use]
    pub fn includes_all_available_tools(&self) -> bool {
        matches!(
            self,
            SpiceToolsOptions::Auto | SpiceToolsOptions::All | SpiceToolsOptions::SearchRegistry
        )
    }

    #[must_use]
    pub fn tools_by_name(&self) -> Vec<&str> {
        match self {
            SpiceToolsOptions::Auto => vec![
                "search",
                "table_schema",
                "sql",
                "list_datasets",
                "get_readiness",
                "get_current_datetime",
            ],
            SpiceToolsOptions::All | SpiceToolsOptions::SearchRegistry => vec![
                "search",
                "table_schema",
                "sql",
                "list_datasets",
                "get_readiness",
                "get_current_datetime",
                "random_sample",
                "sample_distinct_columns",
                "top_n_sample",
            ],
            SpiceToolsOptions::Nsql => vec![
                "table_schema",
                "sql",
                "list_datasets",
                "get_current_datetime",
                "random_sample",
                "sample_distinct_columns",
                "top_n_sample",
            ],
            SpiceToolsOptions::Disabled => vec![],
            SpiceToolsOptions::Specific(t) => t
                .iter()
                // Handle nested groupings. e.g: `spiced_tools: nsql, my_other_tool`.
                .flat_map(|s| match s.parse() {
                    Ok(SpiceToolsOptions::Auto) => SpiceToolsOptions::Auto.tools_by_name(),
                    Ok(SpiceToolsOptions::All | SpiceToolsOptions::SearchRegistry) => {
                        SpiceToolsOptions::All.tools_by_name()
                    }
                    Ok(SpiceToolsOptions::Nsql) => SpiceToolsOptions::Nsql.tools_by_name(),
                    _ => vec![s.as_str()],
                })
                .unique()
                .collect(),
        }
    }
}

impl FromStr for SpiceToolsOptions {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "auto" => Ok(SpiceToolsOptions::Auto),
            "all" => Ok(SpiceToolsOptions::All),
            "search_registry" => Ok(SpiceToolsOptions::SearchRegistry),
            "nsql" => Ok(SpiceToolsOptions::Nsql),
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

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_nested_tool_opts() {
        assert_eq!(
            SpiceToolsOptions::Specific(vec!["nsql".to_string(), "my_other_tool".to_string()])
                .tools_by_name(),
            vec![
                "table_schema",
                "sql",
                "list_datasets",
                "get_current_datetime",
                "random_sample",
                "sample_distinct_columns",
                "top_n_sample",
                "my_other_tool"
            ]
        );

        let opt = SpiceToolsOptions::Specific(vec![
            "nsql".to_string(),
            "my_other_tool".to_string(),
            "sql".to_string(),
        ]);
        let tools = opt.tools_by_name();

        assert_eq!(
            tools.len(),
            tools.iter().unique().count(),
            "'SpiceToolsOptions::tools_by_name' should not produce duplicates"
        );

        assert_eq!(
            SpiceToolsOptions::Specific(vec![
                "search_registry".to_string(),
                "my_other_tool".to_string(),
            ])
            .tools_by_name(),
            SpiceToolsOptions::All
                .tools_by_name()
                .into_iter()
                .chain(["my_other_tool"])
                .collect::<Vec<_>>()
        );

        let auto_opt =
            SpiceToolsOptions::Specific(vec!["auto".to_string(), "my_other_tool".to_string()]);
        let auto_tools = auto_opt.tools_by_name();

        assert!(!auto_tools.contains(&"random_sample"));
        assert!(!auto_tools.contains(&"sample_distinct_columns"));
        assert!(!auto_tools.contains(&"top_n_sample"));
        assert!(auto_tools.contains(&"my_other_tool"));
    }

    #[test]
    fn test_all_tool_opts() {
        assert!(SpiceToolsOptions::All.can_use_tools());
        assert!(SpiceToolsOptions::All.includes_all_available_tools());
        assert!(
            SpiceToolsOptions::All
                .tools_by_name()
                .contains(&"random_sample")
        );

        assert!(matches!(
            "all"
                .parse::<SpiceToolsOptions>()
                .expect("all should parse as a tool option"),
            SpiceToolsOptions::All
        ));
    }

    #[test]
    fn test_search_registry_tool_opts() {
        assert!(SpiceToolsOptions::SearchRegistry.can_use_tools());
        assert!(SpiceToolsOptions::SearchRegistry.includes_all_available_tools());
        assert_eq!(
            SpiceToolsOptions::SearchRegistry.tools_by_name(),
            SpiceToolsOptions::All.tools_by_name()
        );

        assert!(matches!(
            "search_registry"
                .parse::<SpiceToolsOptions>()
                .expect("search_registry should parse as a tool option"),
            SpiceToolsOptions::SearchRegistry
        ));
    }

    #[test]
    fn test_auto_tool_opts_excludes_sampling_tools() {
        let auto_tools = SpiceToolsOptions::Auto.tools_by_name();

        assert!(!auto_tools.contains(&"random_sample"));
        assert!(!auto_tools.contains(&"sample_distinct_columns"));
        assert!(!auto_tools.contains(&"top_n_sample"));
    }

    #[test]
    fn test_search_tool_name_is_specific_tool() {
        let option = "search"
            .parse::<SpiceToolsOptions>()
            .expect("search should parse as a specific tool name");
        assert!(
            matches!(option, SpiceToolsOptions::Specific(tools) if tools == vec!["search".to_string()])
        );
    }
}
