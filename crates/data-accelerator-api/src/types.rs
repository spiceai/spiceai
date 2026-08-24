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

//! Shared types extracted from `dataaccelerator` to break dependency cycles.
//!
//! These types are re-exported from `dataaccelerator` so existing import paths
//! remain unchanged.

use runtime_acceleration::Engine;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use super::{AcceleratorRuntimeConfig, DATA_ACCELERATOR_REGISTRATIONS, DataAccelerator};

// Re-export AccelerationSource from runtime-acceleration so existing paths keep working.
pub use runtime_acceleration::AccelerationSource;

#[derive(Default, Clone)]
pub struct AcceleratorEngineRegistry {
    pub accelerator_engine_registry: Arc<RwLock<HashMap<Engine, Arc<dyn DataAccelerator>>>>,
}

impl AcceleratorEngineRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            accelerator_engine_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_accelerator_engine(&self, engine: Engine) -> Option<Arc<dyn DataAccelerator>> {
        let guard = self.accelerator_engine_registry.read().await;
        let engine = guard.get(&engine);
        match engine {
            Some(engine_ref) => Some(Arc::clone(engine_ref)),
            None => None,
        }
    }

    pub async fn register_accelerator_engine(
        &self,
        engine: Engine,
        accelerator_engine: Arc<dyn DataAccelerator>,
    ) {
        let replaced_engine = {
            let mut registry = self.accelerator_engine_registry.write().await;
            registry.insert(engine, accelerator_engine)
        };

        if let Some(replaced_engine) = replaced_engine
            && let Err(e) = replaced_engine.shutdown().await
        {
            tracing::error!("Failed to shutdown replaced accelerator engine {engine}: {e}");
        }
    }

    /// Builds and registers every engine this build linked, configured for this
    /// `Runtime`.
    ///
    /// `config` is passed to each constructor rather than published somewhere the
    /// constructors can read, so the settings belong to this registry alone — two
    /// `Runtime`s built concurrently in one process cannot see each other's.
    pub async fn register_all(&self, config: &AcceleratorRuntimeConfig) {
        for registration in DATA_ACCELERATOR_REGISTRATIONS {
            self.register_accelerator_engine(
                registration.engine,
                (registration.constructor)(config),
            )
            .await;
        }
    }

    pub async fn unregister_all(&self) {
        let mut registry = self.accelerator_engine_registry.write().await;
        // Shutdown each accelerator before clearing the registry
        for (engine, accelerator) in registry.drain() {
            if let Err(e) = accelerator.shutdown().await {
                tracing::error!("Failed to shutdown accelerator engine {engine}: {e}");
            }
        }
    }
}
