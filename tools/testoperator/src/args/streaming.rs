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

use clap::Parser;

use crate::commands::streaming::SourceType;
use crate::commands::streaming::querysets::QuerySetType;

use super::CommonArgs;

/// Arguments for streaming ingestion benchmarks.
#[derive(Parser, Debug, Clone)]
pub struct StreamingTestArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Streaming source type (e.g., dynamodb-streams, kafka)
    #[arg(long, value_enum)]
    pub source: SourceType,

    /// Query set type (e.g., tpch-lineitem). Determines which datasets to load.
    #[arg(long, value_enum)]
    pub queryset: QuerySetType,

    /// Scale factor for data generation (e.g., 0.01, 0.1, 1.0)
    #[arg(long, default_value = "0.01")]
    pub scale_factor: f64,

    /// Timeout in seconds to wait for ingestion to complete
    #[arg(long, default_value = "300")]
    pub ingestion_timeout: u64,
}
