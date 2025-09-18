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

pub mod embeddings;
pub mod query;
pub mod search;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStatus {
    // The request was not eligible for caching, and thus the cache was not checked.
    CacheDisabled,
    // The request asked to bypass the cache, i.e. via `Cache-Control: no-cache`.
    CacheBypass,
    // The request was a cache hit.
    CacheHit,
    // The request was a cache miss.
    CacheMiss,
}

impl CacheStatus {
    #[must_use]
    pub fn to_header_string(&self) -> Option<String> {
        match self {
            CacheStatus::CacheDisabled => None,
            CacheStatus::CacheBypass => Some("BYPASS".to_string()),
            CacheStatus::CacheHit => Some("HIT".to_string()),
            CacheStatus::CacheMiss => Some("MISS".to_string()),
        }
    }
}
