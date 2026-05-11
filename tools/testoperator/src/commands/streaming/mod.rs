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
//! - `streaming-dynamodb`: Self-contained performance benchmark
//! - `streaming-dynamodb-correctness`: Multi-round CDC data correctness test

pub mod correctness;
pub mod datasets;
pub mod ingestion_runner;
pub mod mutations;
pub mod prepare_runner;
pub mod query_liveness;
pub mod querysets;
pub mod runner;
pub mod sources;
pub(crate) mod traits;
mod utils;
pub mod verification;

pub use correctness::run_correctness;
pub use ingestion_runner::run_ingestion;
pub use prepare_runner::run_prepare_stream;
pub use runner::run_benchmark;
