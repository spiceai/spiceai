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

use std::{sync::Arc, time::Duration};

use cache::{CacheProvider, QueryResultsCacheProvider, SimpleCache};
use datafusion::logical_expr::LogicalPlan;
use spicepod::component::runtime::HashingAlgorithm;

use crate::{Runtime, datafusion::SPICE_RUNTIME_SCHEMA};

const DEFAULT_CACHED_PLANS_MAX_CAPACITY: u64 = 512;

impl Runtime {
    pub async fn init_results_cache(&self) {
        let app = self.app.read().await;
        let Some(app) = app.as_ref() else { return };

        let cache_config = &app.runtime.results_cache;

        if !cache_config.enabled {
            return;
        }

        match QueryResultsCacheProvider::try_new(
            cache_config,
            Box::new([SPICE_RUNTIME_SCHEMA.into(), "information_schema".into()]),
        ) {
            Ok(cache_provider) => {
                tracing::info!("Initialized results cache; {cache_provider}");
                self.datafusion().set_results_cache_provider(cache_provider);
            }
            Err(e) => {
                tracing::warn!("Failed to initialize results cache: {e}");
            }
        }

        // TODO: logical plan cache needs its own configuration?
        let plan_cache_provider: Arc<dyn CacheProvider<LogicalPlan> + Send + Sync> =
            match cache_config.hashing_algorithm {
                HashingAlgorithm::Siphash => Arc::new(SimpleCache::new(
                    DEFAULT_CACHED_PLANS_MAX_CAPACITY,
                    Duration::from_secs(3600),
                    std::hash::RandomState::default(),
                )),
                HashingAlgorithm::Ahash => Arc::new(SimpleCache::new(
                    DEFAULT_CACHED_PLANS_MAX_CAPACITY,
                    Duration::from_secs(3600),
                    ahash::RandomState::default(),
                )),
            };

        self.datafusion()
            .set_logical_plan_cache_provider(plan_cache_provider);
    }
}
