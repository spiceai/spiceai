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

//! Streaming ingestion benchmark commands.
//!
//! This module contains benchmark runners for different streaming sources.
//!
//! ## Commands
//!
//! - `streaming-dynamodb`: Run a single `DynamoDB` streaming benchmark
//! - `dispatch-dynamodb`: Run multi-config `DynamoDB` benchmarks (ingest once, benchmark many)

pub mod datasets;
pub mod dynamodb_dispatch;
pub mod dynamodb_runner;
pub mod mutations;
pub mod query_liveness;
pub mod querysets;
pub mod sources;
mod traits;
mod utils;
pub mod verification;

// Re-export the runner and dispatch entry points
pub use dynamodb_dispatch::run_dispatch;
pub use dynamodb_runner::run_dynamodb;
