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
//! Currently supported:
//! - DynamoDB Streams (`streaming-dynamodb` command)

pub mod datasets;
pub mod dynamodb_runner;
pub mod mutations;
pub mod querysets;
pub mod sources;
mod traits;
mod utils;
pub mod verification;

// Re-export the DynamoDB runner as the main entry point
pub use dynamodb_runner::run_dynamodb;

// Re-export types needed by args
pub use sources::SourceType;

