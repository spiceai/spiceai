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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the memory system used by memory tools (`store_memory`, `load_memory`).
///
/// Memory enables persistent storage and retrieval of information across tool calls,
/// allowing LLMs to maintain context and recall information from previous interactions.
///
/// # Example
///
/// Using the builtin memory engine (default):
/// ```yaml
/// memory:
///   engine: builtin
/// ```
///
/// Using mem0 as the memory backend:
/// ```yaml
/// memory:
///   engine: mem0
///   params:
///     mem0_api_key: ${secrets:MEM0_API_KEY}
///     mem0_user_id: default-user
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct Memory {
    /// The memory engine to use.
    ///
    /// Supported values:
    /// - `builtin` (default): In-memory storage using the configured embeddings and vector search.
    /// - `mem0`: External memory service via mem0.ai API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,

    /// Engine-specific parameters, prefixed with the engine name.
    ///
    /// For `builtin` engine:
    /// - No additional parameters required.
    ///
    /// For `mem0` engine:
    /// - `mem0_api_key`: API key for mem0.ai (required)
    /// - `mem0_user_id`: User identifier for memory scoping (optional, defaults to "default-user")
    /// - `mem0_agent_id`: Agent identifier for memory scoping (optional)
    /// - `mem0_app_id`: Application identifier for memory scoping (optional)
    /// - `mem0_run_id`: Run identifier for memory scoping (optional)
    /// - `mem0_org_id`: Organization identifier (optional)
    /// - `mem0_project_id`: Project identifier (optional)
    /// - `mem0_base_url`: Custom API base URL (optional)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, String>,
}

impl Memory {
    /// Returns the configured engine name, defaulting to "builtin".
    #[must_use]
    pub fn engine(&self) -> &str {
        self.engine.as_deref().unwrap_or("builtin")
    }

    /// Returns true if this memory configuration uses the builtin engine.
    #[must_use]
    pub fn is_builtin(&self) -> bool {
        self.engine() == "builtin"
    }

    /// Returns true if this memory configuration uses the mem0 engine.
    #[must_use]
    pub fn is_mem0(&self) -> bool {
        self.engine() == "mem0"
    }

    /// Gets a parameter value by key.
    #[must_use]
    pub fn get_param(&self, key: &str) -> Option<&String> {
        self.params.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_engine_is_builtin() {
        let memory = Memory::default();
        assert_eq!(memory.engine(), "builtin");
        assert!(memory.is_builtin());
        assert!(!memory.is_mem0());
    }

    #[test]
    fn test_mem0_engine() {
        let memory = Memory {
            engine: Some("mem0".to_string()),
            params: HashMap::from([
                ("mem0_api_key".to_string(), "test-key".to_string()),
                ("mem0_user_id".to_string(), "test-user".to_string()),
            ]),
        };
        assert_eq!(memory.engine(), "mem0");
        assert!(!memory.is_builtin());
        assert!(memory.is_mem0());
        assert_eq!(
            memory.get_param("mem0_api_key"),
            Some(&"test-key".to_string())
        );
        assert_eq!(
            memory.get_param("mem0_user_id"),
            Some(&"test-user".to_string())
        );
    }

    #[test]
    fn test_deserialize_yaml() {
        let yaml = r"
engine: mem0
params:
  mem0_api_key: my-api-key
  mem0_user_id: my-user
";
        let memory: Memory = serde_yaml::from_str(yaml).expect("should deserialize");
        assert_eq!(memory.engine(), "mem0");
        assert_eq!(
            memory.get_param("mem0_api_key"),
            Some(&"my-api-key".to_string())
        );
        assert_eq!(
            memory.get_param("mem0_user_id"),
            Some(&"my-user".to_string())
        );
    }
}
