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

//! Substrait compliance harness for Spice.
//!
//! Mode A runs the IBM TPC-H suite against the workspace `DataFusion` fork
//! (`datafusion-substrait` consumer), over the suite's SF 0.01 CSVs or, with
//! `--scale-factor`, over tables `tpchgen` generates at that scale. Mode B
//! encodes `FlightSQL` `CommandStatementSubstraitPlan` commands and skips
//! execution until a `spiced` fixture exists.

mod compare;
mod datagen;
mod error;
mod mode_a;
mod mode_b;
mod report;
mod schema;
mod suite;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use chrono::Utc;
use clap::{Parser, ValueEnum};
use datafusion::prelude::SessionConfig;
use snafu::{ResultExt, ensure};

use crate::error::Result;
use crate::report::ComplianceReport;
use crate::suite::load_tpch_suite;

/// The suite commit this harness is pinned to: the `spiceai` branch of
/// spiceai/substrait-compliance (IBM `main`, suite files identical to
/// `v0.1.1`, plus the TPC-H q01 shipdate-cutoff correction).
pub const SUITE_REF: &str = "spiceai/substrait-compliance@43d31411c69ef7594887c7d759037bcf8244eeed";

/// spiceai/datafusion git rev from the workspace `[patch.crates-io]`.
pub const DATAFUSION_FORK_REV: &str = "f15d70051ae8d6916f37bd9ce40b596d35adef1c";

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    /// `DataFusion` consumer baseline (IBM `examples/datafusion-rust` shape).
    #[value(name = "mode-a")]
    ModeA,
    /// `FlightSQL` `CommandStatementSubstraitPlan` stub (product path).
    #[value(name = "mode-b")]
    ModeB,
}

impl Mode {
    /// The `--mode` value; also the stem of the default report paths, so a
    /// Mode B run cannot overwrite a Mode A report.
    fn name(self) -> &'static str {
        match self {
            Mode::ModeA => "mode-a",
            Mode::ModeB => "mode-b",
        }
    }

    fn default_output(self, extension: &str) -> PathBuf {
        PathBuf::from(format!(
            "tools/substrait-compliance/results/{}-tpch.{extension}",
            self.name()
        ))
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "spice-substrait-compliance",
    about = "Run IBM/substrait-compliance TPC-H against the Spice DataFusion fork (Mode A) or stub the FlightSQL product path (Mode B)"
)]
struct Args {
    /// IBM test-suite directory (the `test-suites/tpch` folder).
    #[arg(
        long,
        default_value = "tools/substrait-compliance/.ibm/test-suites/tpch"
    )]
    suite: PathBuf,

    /// Which engine path to exercise.
    #[arg(long, value_enum, default_value_t = Mode::ModeA)]
    mode: Mode,

    /// Restrict to a single test id (`q01` … `q22`).
    #[arg(long)]
    query: Option<String>,

    /// Write the JSON report here (default
    /// `tools/substrait-compliance/results/<mode>-tpch.json`).
    #[arg(long)]
    out_json: Option<PathBuf>,

    /// Write the per-query CSV here (default
    /// `tools/substrait-compliance/results/<mode>-tpch.csv`).
    #[arg(long)]
    out_csv: Option<PathBuf>,

    /// `FlightSQL` endpoint used only by Mode B (not contacted yet).
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    flightsql_endpoint: String,

    /// TPC-H scale factor. Omit to run the suite's own SF 0.01 CSVs against
    /// its goldens; set, Mode A generates the tables in memory with `tpchgen`
    /// at this scale and compares against `--expected`.
    #[arg(long, allow_hyphen_values = true)]
    scale_factor: Option<f64>,

    /// Directory of goldens (`q01.csv` … `q22.csv` in the suite's typed CSV
    /// format) that replaces the suite's own. Default with `--scale-factor`:
    /// `tools/substrait-compliance/expected/sf<SF>`.
    #[arg(long)]
    expected: Option<PathBuf>,

    /// Write the `--scale-factor` tables into this directory as pipe-delimited
    /// CSVs (the suite's `data/` layout) and exit without running any case:
    /// the rows `scripts/generate_expected.py` computes goldens from.
    #[arg(long, requires = "scale_factor")]
    write_data: Option<PathBuf>,
}

/// Where a `--scale-factor` run finds its goldens unless `--expected` says.
fn default_expected_dir(scale_factor: f64) -> PathBuf {
    PathBuf::from(format!(
        "tools/substrait-compliance/expected/sf{scale_factor}"
    ))
}

/// Accept `--scale-factor` only when it is a positive finite number.
fn require_scale_factor(scale_factor: Option<f64>) -> Result<()> {
    match scale_factor {
        Some(scale_factor) => datagen::require_positive_scale_factor(scale_factor),
        None => Ok(()),
    }
}

/// Goldens for a `--scale-factor` / `--expected` run. The scale factor is
/// checked first: `0` and `-1` map to directories that are not committed, so
/// a missing-directory check here would hide `InvalidScaleFactor`.
fn resolve_expected_dir(
    scale_factor: Option<f64>,
    expected: Option<PathBuf>,
) -> Result<Option<PathBuf>> {
    require_scale_factor(scale_factor)?;
    let expected_dir = expected.or_else(|| scale_factor.map(default_expected_dir));
    if let Some(dir) = &expected_dir {
        ensure!(
            dir.is_dir(),
            error::MissingExpectedDirSnafu { path: dir.clone() }
        );
    }
    Ok(expected_dir)
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(2)
        }
    }
}

async fn run() -> Result<ExitCode> {
    let args = Args::parse();
    require_scale_factor(args.scale_factor)?;
    if let (Some(dir), Some(scale_factor)) = (&args.write_data, args.scale_factor) {
        return write_data(scale_factor, dir).await;
    }
    let out_json = args
        .out_json
        .clone()
        .unwrap_or_else(|| args.mode.default_output("json"));
    let out_csv = args
        .out_csv
        .clone()
        .unwrap_or_else(|| args.mode.default_output("csv"));
    let expected_dir = resolve_expected_dir(args.scale_factor, args.expected)?;
    let suite = load_tpch_suite(&args.suite, expected_dir.as_deref())?;
    println!(
        "Loaded IBM suite '{}' v{} ({} cases) from {}",
        suite.name,
        suite.version,
        suite.cases.len(),
        suite.root.display()
    );
    // The suite's description states its own scale factor, which a generated
    // run does not use.
    if args.scale_factor.is_none() && !suite.description.is_empty() {
        println!("{}", suite.description);
    }
    println!("Suite: {SUITE_REF}");
    println!("DataFusion fork rev: {DATAFUSION_FORK_REV}");
    let expected_source = expected_dir
        .as_ref()
        .map_or_else(|| "suite".to_string(), |dir| dir.display().to_string());
    match &expected_dir {
        Some(dir) => println!("Expected output: {}", dir.display()),
        None => println!("Expected output: the suite's goldens"),
    }

    let selected = suite::select_cases(&suite.cases, args.query.as_deref())?;

    let start = Utc::now();
    let (engine_name, engine_version, mode_name, results) = match args.mode {
        Mode::ModeA => {
            let engine = match args.scale_factor {
                None => {
                    let data_dir = suite.root.join("data");
                    println!("Data: suite CSVs ({})", data_dir.display());
                    mode_a::ModeAEngine::with_tpch_data(&data_dir).await?
                }
                Some(scale_factor) => {
                    let started = Instant::now();
                    let (engine, row_counts) =
                        mode_a::ModeAEngine::with_generated_data(scale_factor).await?;
                    println!(
                        "Data: TPC-H SF {scale_factor} generated by tpchgen in {:.1}s ({})",
                        started.elapsed().as_secs_f64(),
                        row_counts
                            .iter()
                            .map(|(table, rows)| format!("{table} {rows}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    engine
                }
            };
            let results = engine.run_suite(&selected).await?;
            (
                mode_a::ENGINE_NAME.to_string(),
                mode_a::ENGINE_VERSION.to_string(),
                args.mode.name().to_string(),
                results,
            )
        }
        Mode::ModeB => {
            let engine = mode_b::FlightSqlComplianceEngine::new(&args.flightsql_endpoint);
            for case in &selected {
                // Encode so a missing prost/FlightSQL type fails the stub itself.
                let _ = engine.run_case(case);
            }
            (
                mode_b::ENGINE_NAME.to_string(),
                mode_b::ENGINE_VERSION.to_string(),
                args.mode.name().to_string(),
                mode_b::FlightSqlComplianceEngine::stub_results(&selected),
            )
        }
    };

    for case in &results {
        let mark = match case.status {
            report::TestStatus::Passed => "PASS",
            report::TestStatus::Failed => "FAIL",
            report::TestStatus::Skipped => "SKIP",
            report::TestStatus::Error => "ERROR",
        };
        match &case.error_message {
            Some(msg) => println!("  {mark:5} {}  ({msg})", case.test_id),
            None => println!("  {mark:5} {}", case.test_id),
        }
    }

    let report = ComplianceReport::finish(
        report::ReportMeta {
            suite_name: suite.name,
            suite_version: suite.version,
            engine_name,
            engine_version,
            mode: mode_name,
            suite_ref: SUITE_REF.to_string(),
            datafusion_pin: format!("spiceai/datafusion@{DATAFUSION_FORK_REV}"),
            scale_factor: args.scale_factor.or(suite.scale_factor),
            data_source: match (args.mode, args.scale_factor) {
                (Mode::ModeB, _) => "none",
                (Mode::ModeA, None) => "suite",
                (Mode::ModeA, Some(_)) => "tpchgen",
            }
            .to_string(),
            expected_source,
            start_time: start,
        },
        results,
    );

    println!(
        "\n{}/{}/{}  pass/fail/skip+error  total={}  pass_rate={:.1}%",
        report.passed,
        report.failed,
        report.skipped + report.errored,
        report.total,
        report.pass_rate_pct
    );
    println!(
        "  passed={} failed={} skipped={} errored={}",
        report.passed, report.failed, report.skipped, report.errored
    );

    report.write_json(&out_json)?;
    report.write_csv(&out_csv)?;
    println!("Wrote {}", out_json.display());
    println!("Wrote {}", out_csv.display());

    // Report-only: never fail the process on a low pass rate. A non-zero
    // exit is reserved for harness I/O / load errors (already returned).
    Ok(ExitCode::SUCCESS)
}

/// `--write-data`: generate the tables at `scale_factor` and write them as the
/// suite's `data/` CSVs under `dir`.
async fn write_data(scale_factor: f64, dir: &Path) -> Result<ExitCode> {
    let started = Instant::now();
    let parts = SessionConfig::new().target_partitions();
    let tables = datagen::generate(scale_factor, parts).await?;
    let out = dir.to_path_buf();
    let tables =
        tokio::task::spawn_blocking(move || datagen::write_csv(&tables, &out).map(|()| tables))
            .await
            .context(error::GenerateTaskSnafu)??;
    for table in &tables {
        println!(
            "Wrote {} ({} rows)",
            dir.join(format!("{}.csv", table.table.file_stem)).display(),
            table.num_rows()
        );
    }
    println!(
        "TPC-H SF {scale_factor} generated by tpchgen in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    /// The revision the harness reports must be the one the workspace builds
    /// against, or its results are credited to a `DataFusion` they were not run on.
    #[test]
    fn the_reported_datafusion_rev_is_the_workspace_pin() {
        let manifest = include_str!("../../../Cargo.toml");
        let pinned = manifest
            .lines()
            .find_map(|line| {
                line.strip_prefix(
                    "datafusion = { git = \"https://github.com/spiceai/datafusion.git\", rev = \"",
                )?
                .split('"')
                .next()
            })
            .expect("the workspace patches `datafusion` to a spiceai/datafusion revision");
        assert_eq!(super::DATAFUSION_FORK_REV, pinned);
    }

    /// `--scale-factor 0` / `-1` must not become `MissingExpectedDir`.
    /// Those values map to `expected/sf0` and `expected/sf-1`, which are not
    /// committed, so a directory check before this validation hides the
    /// actionable error (regression for the review on #14522).
    #[test]
    fn invalid_scale_factor_is_rejected_before_the_expected_dir_is_resolved() {
        for scale_factor in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let err = super::resolve_expected_dir(Some(scale_factor), None)
                .expect_err("invalid scale factor");
            assert!(
                matches!(err, crate::error::Error::InvalidScaleFactor { .. }),
                "{scale_factor}: {err}"
            );
            let dir = super::default_expected_dir(scale_factor);
            assert!(
                !dir.is_dir(),
                "{scale_factor}: {} must not exist; a directory check here would hide InvalidScaleFactor",
                dir.display()
            );
        }
    }

    #[test]
    fn a_leading_minus_on_scale_factor_is_a_value_not_a_flag() {
        use clap::Parser as _;
        let args =
            super::Args::try_parse_from(["spice-substrait-compliance", "--scale-factor", "-1"])
                .expect("clap accepts -1 as the scale factor");
        assert_eq!(args.scale_factor, Some(-1.0));
        let err = super::require_scale_factor(args.scale_factor).expect_err("invalid scale factor");
        assert!(
            matches!(err, crate::error::Error::InvalidScaleFactor { .. }),
            "{err}"
        );
    }

    #[test]
    fn a_missing_expected_dir_is_still_reported_when_the_scale_factor_is_valid() {
        let path =
            std::path::PathBuf::from("tools/substrait-compliance/expected/sf-does-not-exist");
        let err = super::resolve_expected_dir(Some(1.0), Some(path.clone()))
            .expect_err("missing expected dir");
        assert!(
            matches!(err, crate::error::Error::MissingExpectedDir { path: ref p } if *p == path),
            "{err}"
        );
    }
}
