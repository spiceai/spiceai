/*
Copyright 2026 The Spice.ai OSS Authors

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
//! The builtin memory engine uses SQL-based storage with a memory connector dataset.

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
/// Returns the builtin memory engine which uses SQL-based storage.
/// Takes a reference to avoid unnecessary Arc cloning.
pub async fn get_memory_engine(
    rt: &Arc<Runtime>,
) -> Result<Arc<dyn MemoryEngine>, Box<dyn std::error::Error + Send + Sync>> {
    use super::builtin::BuiltinMemoryEngine;

    Ok(Arc::new(BuiltinMemoryEngine::new(rt)))
}
