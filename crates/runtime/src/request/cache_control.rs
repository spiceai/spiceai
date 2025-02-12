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

use http::{header::CACHE_CONTROL, HeaderMap};

#[derive(Debug, Clone, Copy, Default)]
pub enum CacheControl {
    #[default]
    Cache,
    NoCache,
}

impl CacheControl {
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let Some(cache_control) = headers.get(CACHE_CONTROL) else {
            return Self::Cache;
        };
        let Ok(cache_control_str) = cache_control.to_str() else {
            return Self::Cache;
        };

        match cache_control_str {
            "no-cache" => Self::NoCache,
            _ => Self::Cache,
        }
    }
}
