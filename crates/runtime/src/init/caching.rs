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

use cache::{Caching, QueryResultsCacheProvider, SimpleCache, get_hash_builder, lru_cache};
use spicepod::component::caching::{CacheConfig, Caching as CachingConfig, SQLResultsCacheConfig};
use util::in_tracing_context;

use crate::{Runtime, datafusion::SPICE_RUNTIME_SCHEMA};

const DEFAULT_CACHED_PLANS_MAX_CAPACITY: u64 = 512;

impl Runtime {
    #[must_use]
    pub fn init_caching(cache_config: Option<&CachingConfig>) -> Arc<Caching> {
        let Some(cache_config) = cache_config else {
            return Arc::new(Caching::new());
        };

        let mut caching = Caching::new();

        let sql_results_config = cache_config
            .sql_results
            .clone()
            .unwrap_or(SQLResultsCacheConfig::default());
        let search_results_config = cache_config
            .search_results
            .clone()
            .unwrap_or(CacheConfig::default());

        if sql_results_config.enabled {
            match QueryResultsCacheProvider::try_new(
                &sql_results_config,
                Box::new([SPICE_RUNTIME_SCHEMA.into(), "information_schema".into()]),
            ) {
                Ok(cache_provider) => {
                    in_tracing_context(|| {
                        tracing::info!("Initialized results cache; {cache_provider}");
                    });
                    caching = caching.with_results_cache(Arc::new(cache_provider));
                }
                Err(e) => {
                    in_tracing_context(|| {
                        tracing::error!("Failed to initialize results cache: {e}");
                    });
                }
            }
        }

        match get_hash_builder(sql_results_config.hashing_algorithm) {
            Ok(hash_builder) => {
                let plans_cache_provider = Arc::new(SimpleCache::new(
                    DEFAULT_CACHED_PLANS_MAX_CAPACITY,
                    Duration::from_secs(3600),
                    hash_builder,
                ));
                caching = caching.with_plans_cache(plans_cache_provider);
            }
            Err(e) => {
                in_tracing_context(|| {
                    tracing::error!("Failed to initialize plans cache: {e}");
                });
            }
        }

        if search_results_config.enabled {
            match lru_cache::build_from_config(&search_results_config) {
                Ok(cache_provider) => {
                    in_tracing_context(|| {
                        tracing::info!("Initialized search results cache;"); // TODO: update to include max size and ttl. https://github.com/spiceai/spiceai/issues/6019
                    });
                    caching = caching.with_search_cache(cache_provider);
                }
                Err(e) => {
                    in_tracing_context(|| {
                        tracing::error!("Failed to initialize search results cache: {e}");
                    });
                }
            }
        }

        Arc::new(caching)
    }
}
