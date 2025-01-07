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

use governor::Quota;
use std::num::NonZeroU32;

pub struct RateLimits {
    pub flight_write_limit: Quota,
}

impl RateLimits {
    #[must_use]
    pub fn new() -> Self {
        RateLimits::default()
    }

    #[must_use]
    pub fn with_flight_write_limit(mut self, rate_limit: Quota) -> Self {
        self.flight_write_limit = rate_limit;
        self
    }
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            // Allow 100 Flight DoPut requests every 60 seconds by default
            flight_write_limit: Quota::per_minute(NonZeroU32::new(100).unwrap_or_else(|| {
                unreachable!("100 is non-zero and should always successfully convert to NonZeroU32")
            })),
        }
    }
}
