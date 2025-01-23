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

pub mod flight;
pub mod gh_utils;
pub mod metrics;
pub mod queries;
pub mod spiced;
pub mod spicepod_utils;
pub mod spicetest;
pub mod utils;

use std::fmt::Display;

pub use anyhow;
pub use app;
pub use arrow;
pub use flight_client;
pub use futures;
pub use octocrab;
pub use rustls;
pub use serde_yaml;
pub use spicepod;

#[derive(Debug, Clone, Copy)]
pub enum TestType {
    Throughput,
    Load,
    Benchmark,
    DataConsistency,
    HttpConsistency,
    HttpOverhead,
}

impl TestType {
    #[must_use]
    pub fn workflow(&self) -> &str {
        match self {
            TestType::Throughput => "testoperator_run_throughput.yml",
            TestType::Load => "testoperator_run_load.yml",
            TestType::Benchmark => "testoperator_run_bench.yml",
            TestType::DataConsistency => "testoperator_run_data_consistency.yml",
            TestType::HttpConsistency => "testoperator_run_http_consistency.yml",
            TestType::HttpOverhead => "testoperator_run_http_overhead.yml",
        }
    }
}

impl Display for TestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestType::Throughput => write!(f, "throughput"),
            TestType::Load => write!(f, "load"),
            TestType::Benchmark => write!(f, "benchmark"),
            TestType::DataConsistency => write!(f, "data_consistency"),
            TestType::HttpConsistency => write!(f, "http_consistency"),
            TestType::HttpOverhead => write!(f, "http_overhead"),
        }
    }
}
