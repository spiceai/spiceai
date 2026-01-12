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

//! Mem0 memory engine implementation.

use async_trait::async_trait;
use runtime_secrets::ParamStr;
use secrecy::SecretString;
use serde_json::{Value, json};
use spicepod::component::memory::Memory;
use std::{collections::HashMap, sync::Arc};

use crate::{
    Runtime,
    tools::mem0::client::{
        AddMemoryRequest, AddMemoryResponse, GetMemoriesRequest, Mem0Client, Mem0Config, Message,
    },
};

use super::engine::MemoryEngine;

/// Memory engine that uses Mem0 for external memory storage.
pub struct Mem0MemoryEngine {
    client: Arc<Mem0Client>,
}

impl Mem0MemoryEngine {
    /// Create a new Mem0 memory engine from the memory configuration.
    pub async fn from_config(
        rt: &Arc<Runtime>,
        config: &Memory,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Resolve secrets in params
        let secrets_arc = rt.secrets();
        let secrets = secrets_arc.read().await;
        let mut resolved_params: HashMap<String, SecretString> = HashMap::new();

        for (key, value) in &config.params {
            let resolved = secrets.inject_secrets(key, ParamStr(value)).await;
            resolved_params.insert(key.clone(), resolved);
        }

        drop(secrets);

        let mem0_config = Mem0Config::from_params(&resolved_params)
            .map_err(|e| format!("Failed to create Mem0 config from memory params: {e}"))?;

        let client = Mem0Client::new(mem0_config)
            .map_err(|e| format!("Failed to create Mem0 client: {e}"))?;

        Ok(Self {
            client: Arc::new(client),
        })
    }
}

#[async_trait]
impl MemoryEngine for Mem0MemoryEngine {
    async fn store(
        &self,
        value: &str,
        _created_by: Option<&str>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: value.to_string(),
            }],
            user_id: self.client.default_user_id().map(ToString::to_string),
            async_mode: false,
            ..Default::default()
        };

        let response = self
            .client
            .add_memories(request)
            .await
            .map_err(|e| format!("Failed to store memory in Mem0: {e}"))?;

        match response {
            AddMemoryResponse::Sync(events) => Ok(json!({
                "success": true,
                "memories_added": events.len(),
                "events": events
            })),
            AddMemoryResponse::Async(pending) => Ok(json!({
                "success": true,
                "status": "pending",
                "pending_events": pending
            })),
        }
    }

    async fn load(
        &self,
        _last_interval: &str,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        // Mem0 doesn't have time-based filtering in the same way as builtin.
        // We'll get all memories and let the caller filter if needed.
        // Build filter for user_id if configured
        let filters = if let Some(user_id) = self.client.default_user_id() {
            json!({"user_id": user_id})
        } else {
            json!({})
        };

        let request = GetMemoriesRequest {
            filters,
            page: None,
            page_size: None,
            org_id: None,
            project_id: None,
        };

        let memories = self
            .client
            .get_memories(request)
            .await
            .map_err(|e| format!("Failed to load memories from Mem0: {e}"))?;

        // Convert to a simple list of memory values
        let values: Vec<String> = memories.iter().map(|m| m.memory.clone()).collect();

        Ok(json!(values))
    }
}
