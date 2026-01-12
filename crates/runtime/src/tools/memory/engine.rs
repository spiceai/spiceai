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

//! Memory engine abstraction for `store_memory` and `load_memory` tools.
//!
//! This module provides a unified interface for memory storage backends.
//! The engine can be configured in the spicepod:
//!
//! ```yaml
//! memory:
//!   engine: builtin  # or "mem0"
//!   params:
//!     api_key: ${secrets:MEM0_API_KEY}  # for mem0 engine
//! ```

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

use crate::Runtime;

/// Trait for memory engine implementations.
#[async_trait]
pub trait MemoryEngine: Send + Sync {
    /// Store a memory value.
    ///
    /// # Arguments
    /// * `value` - The memory content to store
    /// * `created_by` - Optional identifier for who/what created the memory
    ///
    /// # Returns
    /// A JSON value containing the result of the store operation.
    async fn store(
        &self,
        value: &str,
        created_by: Option<&str>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;

    /// Load memories based on a time interval.
    ///
    /// # Arguments
    /// * `last_interval` - Duration string in ISO 8601 format (e.g., "1h", "30m")
    ///
    /// # Returns
    /// A JSON value containing the loaded memories.
    async fn load(
        &self,
        last_interval: &str,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
}

/// Get the configured memory engine for the runtime.
///
/// Returns the appropriate engine based on the `memory.engine` configuration:
/// - `builtin` (default): SQL-based storage using a memory connector dataset
/// - `mem0`: External memory service via mem0.ai API
pub async fn get_memory_engine(
    rt: Arc<Runtime>,
) -> Result<Arc<dyn MemoryEngine>, Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(feature = "mem0")]
    use super::mem0_engine::Mem0MemoryEngine;
    use super::{builtin::BuiltinMemoryEngine, get_memory_config};

    let memory_config = get_memory_config(&rt).await;
    let engine_name = memory_config.as_ref().map_or("builtin", |m| m.engine());

    match engine_name {
        "builtin" => Ok(Arc::new(BuiltinMemoryEngine::new(rt))),
        #[cfg(feature = "mem0")]
        "mem0" => {
            let config = memory_config.ok_or("mem0 engine requires memory configuration")?;
            let engine = Mem0MemoryEngine::from_config(&rt, &config).await?;
            Ok(Arc::new(engine))
        }
        #[cfg(not(feature = "mem0"))]
        "mem0" => Err("mem0 feature is not enabled. Compile with --features mem0".into()),
        other => {
            Err(format!("Unknown memory engine: {other}. Supported engines: builtin, mem0").into())
        }
    }
}
