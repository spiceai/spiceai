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

use std::num::NonZeroU32;

use governor::Quota;

// Maximum number of concurrently running requests.
// The overall request rate is controlled by the rate_limiter.
const DEFAULT_MAX_CONCURRENT_INVOCATIONS: usize = 40;

#[derive(Debug)]
pub struct BedrockRateLimitConfigBuilder {
    requests_per_minute_limit: Option<u32>,
    max_concurrent_invocations: Option<usize>,
}

impl Default for BedrockRateLimitConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BedrockRateLimitConfigBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            requests_per_minute_limit: None,
            max_concurrent_invocations: None,
        }
    }

    #[must_use]
    pub fn requests_per_minute(&mut self, limit: u32) -> &Self {
        self.requests_per_minute_limit = Some(limit);
        self
    }

    #[must_use]
    pub fn max_concurrent_invocations(&mut self, limit: usize) -> &Self {
        self.max_concurrent_invocations = Some(limit);
        self
    }

    #[must_use]
    pub fn build(self) -> BedrockRateLimitConfig {
        BedrockRateLimitConfig {
            requests_per_minute_limit: self.requests_per_minute_limit.unwrap_or(1_500),
            max_concurrent_invocations: self
                .max_concurrent_invocations
                .unwrap_or(DEFAULT_MAX_CONCURRENT_INVOCATIONS),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BedrockRateLimitConfig {
    pub requests_per_minute_limit: u32,
    pub max_concurrent_invocations: usize,
}

impl BedrockRateLimitConfig {
    #[must_use]
    pub fn to_quota(&self) -> Quota {
        Quota::per_minute(
            NonZeroU32::new(self.requests_per_minute_limit).unwrap_or_else(|| {
                unreachable!(
                    "requests_per_minute_limit is u32 and should always successfully convert to NonZeroU32"
                )
            }),
        )
    }
}
