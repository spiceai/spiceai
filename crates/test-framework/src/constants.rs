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

//! Constants for test-framework and testoperator, including default URLs and endpoints

/// Default HTTP endpoint for spiced runtime
pub const HTTP_BASE_URL: &str = "http://localhost:8090";

/// Default Flight SQL endpoint for spiced runtime
pub const FLIGHT_URL: &str = "http://localhost:50051";

/// Default metrics endpoint for spiced runtime (when --metrics is enabled)
pub const METRICS_URL: &str = "http://localhost:9090/metrics";

/// Health check endpoint path (relative to `HTTP_BASE_URL`)
pub const HEALTH_ENDPOINT: &str = "/health";

/// Ready check endpoint path (relative to `HTTP_BASE_URL`)
pub const READY_ENDPOINT: &str = "/v1/ready";

/// SQL query endpoint path (relative to `HTTP_BASE_URL`)
pub const SQL_ENDPOINT: &str = "/v1/sql";

/// Search endpoint path (relative to `HTTP_BASE_URL`)
pub const SEARCH_ENDPOINT: &str = "/v1/search";

/// Evals endpoint path template (relative to `HTTP_BASE_URL`)
/// Use `format!("{HTTP_BASE_URL}/v1/evals/{eval_name}")`
pub const EVALS_ENDPOINT_PREFIX: &str = "/v1/evals";

/// API base path (relative to `HTTP_BASE_URL`)
pub const API_BASE_PATH: &str = "/v1";
