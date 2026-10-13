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
    #[snafu(display(
        "Failed to resolve '{}' against the working directory: {source}",
        path.display()
    ))]
    AbsolutePath {
        path: PathBuf,
        source: std::io::Error,
    },

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

    #[snafu(display(
        "Failed to load expected output '{}', so the case cannot be certified: {source}",
        path.display()
    ))]
    InvalidGolden {
        path: PathBuf,
        source: crate::compare::ParseTypedCsvError,
    },

    #[snafu(display(
        "Test '{test_id}' declares expected output '{}' but the file is missing; a case without its golden \
        would be reported SKIPPED and silently weaken the run. Check the suite checkout \
        (scripts/fetch-ibm.sh) or remove the declaration",
        path.display()
    ))]
    MissingGolden { test_id: String, path: PathBuf },

    #[snafu(display(
        "Expected-output directory '{}' does not exist, so no case can be certified. Generate \
         it with `tools/substrait-compliance/scripts/generate_expected.py` or pass \
         `--expected <dir>`",
        path.display()
    ))]
    MissingExpectedDir { path: PathBuf },

    #[snafu(display(
        "Test '{test_id}' has no expected output '{}'. Every case needs one in an `--expected` \
         directory: a case without its golden would be reported SKIPPED and silently weaken the \
         run. Regenerate the directory with `tools/substrait-compliance/scripts/generate_expected.py`",
        path.display()
    ))]
    MissingExpectedFile { test_id: String, path: PathBuf },

    #[snafu(display("Unknown TPC-H table '{name}' referenced by test '{test_id}'"))]
    UnknownTable { name: String, test_id: String },

    #[snafu(display(
        "TPC-H scale factor {scale_factor} is not a positive number. Pass `--scale-factor` a \
         value above 0, such as 1"
    ))]
    InvalidScaleFactor { scale_factor: f64 },

    #[snafu(display(
        "Generated TPC-H key {value} in '{table}.{column}' does not fit the 32-bit key type the \
         suite's plans declare, so this scale factor cannot run. Lower `--scale-factor`"
    ))]
    KeyOutOfRange {
        table: String,
        column: String,
        value: i64,
    },

    #[snafu(display(
        "Generated row for '{table}' has {actual} values but the table has {expected} columns"
    ))]
    RowWidth {
        table: String,
        actual: usize,
        expected: usize,
    },

    #[snafu(display(
        "Generated value for '{table}.{column}' does not match the column's type {data_type}"
    ))]
    CellTypeMismatch {
        table: String,
        column: String,
        data_type: String,
    },

    #[snafu(display(
        "Column '{table}.{column}' has type {data_type}, which the TPC-H data generator does \
         not produce"
    ))]
    UnsupportedColumnType {
        table: String,
        column: String,
        data_type: String,
    },

    #[snafu(display("Failed to format a generated '{table}.{column}' value"))]
    FormatText { table: String, column: String },

    #[snafu(display("Failed to build a generated '{table}' record batch: {source}"))]
    BuildBatch {
        table: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("A TPC-H data generation task failed: {source}"))]
    GenerateTask { source: tokio::task::JoinError },

    #[snafu(display("Failed to write generated rows to '{}': {source}", path.display()))]
    WriteCsv {
        path: PathBuf,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("Failed to register generated table '{table}': {source}"))]
    RegisterGeneratedTable {
        table: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "No TPC-H case matches `--query` '{query}'. Known ids: {known}. \
         Pass a listed id or omit `--query` to run the full suite"
    ))]
    UnknownQuery { query: String, known: String },

    #[snafu(display(
        "The TPC-H suite loaded 0 cases, so nothing can be certified. \
         Check the suite checkout (`scripts/fetch-ibm.sh`)"
    ))]
    EmptySuite,

    #[snafu(display(
        "Failed to register table '{table}' from '{}': {source}",
        path.display()
    ))]
    RegisterTable {
        table: String,
        path: PathBuf,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Mode B could not serve the TPC-H tables through spiced: {detail}"))]
    ModeBServe { detail: String },

    #[snafu(display(
        "Mode B needs generated tables to serve through spiced; pass --scale-factor, and --spiced-path for the spiced binary"
    ))]
    ModeBNeedsGeneratedData,

    #[snafu(display("Failed to write parquet '{}': {source}", path.display()))]
    WriteParquet {
        path: PathBuf,
        source: datafusion::parquet::errors::ParquetError,
    },

    #[snafu(display(
        "`{flag}` applies to Mode B only, and Mode A would ignore it. Pass `--mode mode-b`, or \
         drop `{flag}`"
    ))]
    ModeBOnlyFlag { flag: &'static str },

    #[snafu(display(
        "`{flag}` configures an acceleration, and `--acceleration-engine none` serves the tables \
         federated, without one. Pass an acceleration engine, or drop `{flag}`"
    ))]
    AccelerationOnlyFlag { flag: &'static str },

    #[snafu(display(
        "The baseline report '{}' passed none of the cases this run selected, so comparing \
         against it would check nothing. Pass a baseline that passed them",
        path.display()
    ))]
    BaselineVacuous { path: PathBuf },

    #[snafu(display(
        "Failed to read the baseline report '{}': {detail}",
        path.display()
    ))]
    Baseline { path: PathBuf, detail: String },
}
