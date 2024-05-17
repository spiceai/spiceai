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

use arrow::array::RecordBatch;
use async_trait::async_trait;
use byte_unit::Byte;
use lru_cache::LruCache;
use snafu::{ResultExt, Snafu};
use spicepod::component::runtime::ResultsCache;

mod lru_cache;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to access cache: {}", source))]
    FailedToAccessCache {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to parse cache max size: {}", source))]
    FailedToParseCacheMaxSize { source: byte_unit::ParseError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
pub trait QueryResultCache {
    async fn get(&mut self, key: u64) -> AnyErrorResult<Option<Vec<RecordBatch>>>;
    async fn put(&mut self, key: u64, batches: Vec<RecordBatch>) -> AnyErrorResult<()>;
}

pub struct QueryResultCacheProvider {
    cache: Box<dyn QueryResultCache + Send + Sync>,
}

impl QueryResultCacheProvider {
    /// # Errors
    ///
    /// Will return `Err` if method fails to parse cache params or to create the cache
    pub fn new(_config: &ResultsCache) -> Result<Self> {
        let cache_max_size = Byte::parse_str("128mb", true)
            .context(FailedToParseCacheMaxSizeSnafu)?
            .as_u64();

        let cache_provider = QueryResultCacheProvider {
            cache: Box::new(LruCache::new(cache_max_size)),
        };

        Ok(cache_provider)
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn get(&mut self, key: u64) -> Result<Option<Vec<RecordBatch>>> {
        self.cache.get(key).await.context(FailedToAccessCacheSnafu)
    }

    /// # Errors
    ///
    /// Will return `Err` if method fails to access the cache
    pub async fn put(&mut self, key: u64, batches: Vec<RecordBatch>) -> Result<()> {
        self.cache
            .put(key, batches)
            .await
            .context(FailedToAccessCacheSnafu)
    }
}
