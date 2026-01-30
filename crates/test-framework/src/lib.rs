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

#![allow(clippy::missing_errors_doc)]

pub mod app_utils;
#[expect(clippy::expect_used, clippy::missing_panics_doc)]
// this is our test framework, used in tests - expect is acceptable
pub mod arrow_record_batch_gen;
pub mod constants;
pub mod flight;
pub mod gh_utils;
pub mod git;
pub mod metrics;
pub mod object_store;
pub mod pki;
pub mod process;
pub mod queries;
pub mod snapshot;
pub mod spiced;
pub mod spicepod_utils;
pub mod spicetest;
pub mod telemetry;
pub mod utils;

use std::fmt::Display;

pub use anyhow;
pub use app;
pub use arrow;
pub use flight_client;
pub use futures;
pub use octocrab;
pub use opentelemetry;
pub use opentelemetry_sdk;
pub use rustls;
pub use serde_yaml;
pub use spicepod;
pub use tokio_util;

#[derive(Debug, Clone, Copy)]
pub enum TestType {
    Throughput,
    Load,
    Benchmark,
    Append,
    DataConsistency,
    Search,
    TextToSql,
}

impl TestType {
    #[must_use]
    pub fn workflow(&self) -> &str {
        match self {
            TestType::Throughput => "testoperator_run_throughput.yml",
            TestType::Load => "testoperator_run_load.yml",
            TestType::Benchmark => "testoperator_run_bench.yml",
            TestType::Append => "testoperator_run_append.yml",
            TestType::DataConsistency => "testoperator_run_data_consistency.yml",
            TestType::Search => "testoperator_run_search.yml",
            TestType::TextToSql => "testoperator_run_texttosql.yml",
        }
    }
}

impl Display for TestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestType::Throughput => write!(f, "throughput"),
            TestType::Load => write!(f, "load"),
            TestType::Benchmark => write!(f, "benchmark"),
            TestType::Append => write!(f, "append"),
            TestType::DataConsistency => write!(f, "data_consistency"),
            TestType::Search => write!(f, "search"),
            TestType::TextToSql => write!(f, "text_to_sql"),
        }
    }
}
