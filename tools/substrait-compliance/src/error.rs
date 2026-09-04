/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use std::path::PathBuf;

use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to read '{}': {source}", path.display()))]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write '{}': {source}", path.display()))]
    WriteFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse YAML at '{}': {source}", path.display()))]
    YamlParse { path: PathBuf, source: yaml::Error },

    #[snafu(display("Failed to serialize report JSON: {source}"))]
    JsonSerialize { source: serde_json::Error },

    #[snafu(display("TPC-H suite directory '{}' is missing `{name}`", path.display()))]
    SuitePathMissing { path: PathBuf, name: String },

    #[snafu(display("Unknown TPC-H table '{name}' referenced by test '{test_id}'"))]
    UnknownTable { name: String, test_id: String },

    #[snafu(display(
        "Failed to register table '{table}' from '{}': {source}",
        path.display()
    ))]
    RegisterTable {
        table: String,
        path: PathBuf,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Mode B FlightSQL engine is a stub: start `spiced` with TPC-H datasets and invoke CommandStatementSubstraitPlan (see tools/substrait-compliance/README.md). {detail}"
    ))]
    ModeBNotImplemented { detail: String },
}
