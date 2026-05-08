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

pub mod allowlist;
pub mod analyzer_rule;
pub mod composed_catalog;
pub mod config;
pub mod dialect;
pub mod error;
pub mod execution_plan;
pub mod extension;
pub mod join_accumulator;
pub mod managed_runtime;
pub mod optimizer_rule;
pub mod param_utils;
pub mod schema_provider;
pub mod sort_columns;
pub mod url_table;
use snafu::prelude::*;

pub const SPICE_DEFAULT_CATALOG: &str = "spice";
pub const SPICE_DEFAULT_SCHEMA: &str = "public";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid children count. Expected only one input, got {children_count}."))]
    InvalidChildrenCount { children_count: usize },
}
