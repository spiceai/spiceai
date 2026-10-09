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
//! `--scale-factor`, over tables `tpchgen` generates at that scale. Mode B runs
//! the same plans through `spiced` — `FlightSQL` `CommandStatementSubstraitPlan`
//! — over those generated tables, accelerated and laid out as asked.

mod compare;
mod datagen;
mod error;
mod mode_a;
mod mode_b;
mod plan_names;
mod report;
mod schema;
mod suite;

use std::num::NonZeroU32;
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::{Parser, ValueEnum};
use datafusion::prelude::SessionConfig;
use snafu::{ResultExt, ensure};
use test_framework::layout::Layout;
use test_framework::spicepod::acceleration::Mode as Mode_;

use crate::error::Result;
use crate::report::ComplianceReport;
use crate::suite::load_tpch_suite;

/// The suite commit this harness is pinned to: the `spiceai` branch of
/// spiceai/substrait-compliance (IBM `main`, suite files identical to
/// `v0.1.1`, plus the TPC-H q01 shipdate-cutoff correction).
pub const SUITE_REF: &str = "spiceai/substrait-compliance@43d31411c69ef7594887c7d759037bcf8244eeed";

/// spiceai/datafusion git rev from the workspace `[patch.crates-io]`.
pub const DATAFUSION_FORK_REV: &str = "eea120e236447a70d7c8802401b3ed3ee24f0980";

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    /// `DataFusion` consumer baseline (IBM `examples/datafusion-rust` shape).
    #[value(name = "mode-a")]
    ModeA,
    /// `spiced` over `FlightSQL` `CommandStatementSubstraitPlan` (the product path).
    #[value(name = "mode-b")]
    ModeB,
}

/// How Mode B's datasets are stored.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum AccelerationMode {
    File,
    Memory,
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
    about = "Run IBM/substrait-compliance TPC-H against the Spice DataFusion fork (Mode A) or through spiced's FlightSQL product path (Mode B)"
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

    /// Mode B: the `spiced` binary that serves the generated tables: a path,
    /// or a bare name such as `spiced` to find on `PATH`.
    #[arg(long)]
    spiced_path: Option<PathBuf>,

    /// Mode B: the acceleration engine of every TPC-H dataset (`cayenne`,
    /// `duckdb`, `arrow`, `sqlite`, …), or `none` to serve the parquet files
    /// federated.
    #[arg(long, default_value = "cayenne")]
    acceleration_engine: String,

    /// Mode B: how the accelerated datasets are stored (default `file`).
    #[arg(long, value_enum)]
    acceleration_mode: Option<AccelerationMode>,

    /// Mode B: an acceleration layout for every dataset (`testoperator
    /// --layout`): features from `primary_key`, `indexes`, `sort`, `cluster`,
    /// `time_column` and `partition`, joined by commas.
    #[arg(long)]
    layout: Option<Layout>,

    /// Mode B: where to write the generated tables as parquet. Default: a
    /// temporary directory.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Mode B: seconds to wait for `spiced` to load the tables.
    #[arg(long, default_value_t = 900)]
    ready_wait: u64,

    /// Mode B: how many times to run each plan. Every execution is compared
    /// with the golden, so an answer that changes once a cache or an index is
    /// warm fails the case.
    #[arg(long, default_value_t = NonZeroU32::MIN)]
    iterations: NonZeroU32,

    /// An earlier report of the same suite. The run fails (exit 1) when a case
    /// that passed there does not pass here: how a layout run is held to the
    /// results of the default layout.
    #[arg(long)]
    baseline: Option<PathBuf>,

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
    let expected_dir = resolve_expected_dir(args.scale_factor, args.expected.clone())?;
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
            reject_mode_b_flags(&args)?;
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
            let (Some(scale_factor), Some(spiced_path)) = (args.scale_factor, &args.spiced_path)
            else {
                return error::ModeBNeedsGeneratedDataSnafu.fail();
            };
            // `spiced` runs in a temporary directory of its own, so neither a path
            // to its binary nor the data directory its datasets read may be
            // relative to this process's working directory.
            let spiced_path = spiced_program(spiced_path)?;
            let options = mode_b::ServingOptions {
                spiced_path,
                acceleration: acceleration_options(&args)?,
                ready_wait: Duration::from_secs(args.ready_wait),
            };
            let started = Instant::now();
            let parts = SessionConfig::new().target_partitions();
            let tables = datagen::generate(scale_factor, parts).await?;
            let temp_dir;
            let data_dir = if let Some(dir) = &args.data_dir {
                std::path::absolute(dir).context(error::AbsolutePathSnafu { path: dir })?
            } else {
                temp_dir = tempfile::tempdir().context(error::WriteFileSnafu {
                    path: std::env::temp_dir(),
                })?;
                temp_dir.path().to_path_buf()
            };
            println!(
                "Serving TPC-H SF {scale_factor} (tpchgen, {:.1}s) from {} through {}",
                started.elapsed().as_secs_f64(),
                data_dir.display(),
                options.engine_description()
            );
            let mut engine = mode_b::SpicedEngine::start(&options, &tables, &data_dir).await?;
            let results = engine.run_suite(&selected, args.iterations).await;
            (
                options.engine_description(),
                engine.version(),
                args.mode.name().to_string(),
                results,
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
            data_source: if args.scale_factor.is_some() {
                "tpchgen"
            } else {
                "suite"
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

    // A low pass rate alone never fails the process: the suite still has cases
    // the consumer cannot run. A case that passed in `--baseline` and does not
    // pass now does, since the only difference is what this run varied.
    if let Some(baseline) = &args.baseline {
        let regressions = baseline_regressions(baseline, &report.results)?;
        if !regressions.is_empty() {
            println!(
                "\n{} case(s) passed in {} but not here:",
                regressions.len(),
                baseline.display()
            );
            for (test_id, status) in &regressions {
                println!("  {test_id}: {status}");
            }
            return Ok(ExitCode::FAILURE);
        }
        println!("No case that passed in {} fails here", baseline.display());
    }
    Ok(ExitCode::SUCCESS)
}

/// The program Mode B starts `spiced` from. A bare name such as `spiced` is
/// left for the `PATH` lookup a shell would do; any other path is made
/// absolute, so the temporary directory `spiced` runs in cannot change which
/// file it names.
fn spiced_program(spiced_path: &Path) -> Result<PathBuf> {
    let mut components = spiced_path.components();
    if let (Some(Component::Normal(_)), None) = (components.next(), components.next()) {
        return Ok(spiced_path.to_path_buf());
    }
    std::path::absolute(spiced_path).context(error::AbsolutePathSnafu { path: spiced_path })
}

/// Mode B's acceleration of every dataset, or `None` to serve them federated.
/// A federated run has no acceleration to store or lay out, so it refuses a
/// flag that configures one rather than run a weaker test than was asked for.
fn acceleration_options(args: &Args) -> Result<Option<mode_b::AccelerationOptions>> {
    if args.acceleration_engine == "none" {
        let flag = if args.acceleration_mode.is_some() {
            Some("--acceleration-mode")
        } else if args.layout.is_some() {
            Some("--layout")
        } else {
            None
        };
        return match flag {
            Some(flag) => error::AccelerationOnlyFlagSnafu { flag }.fail(),
            None => Ok(None),
        };
    }
    Ok(Some(mode_b::AccelerationOptions {
        engine: args.acceleration_engine.clone(),
        mode: match args.acceleration_mode.unwrap_or(AccelerationMode::File) {
            AccelerationMode::File => Mode_::File,
            AccelerationMode::Memory => Mode_::Memory,
        },
        layout: args.layout.clone(),
    }))
}

/// Refuse a Mode A run that names a Mode B-only flag, which it would ignore.
fn reject_mode_b_flags(args: &Args) -> Result<()> {
    let flag = if args.spiced_path.is_some() {
        Some("--spiced-path")
    } else if args.acceleration_mode.is_some() {
        Some("--acceleration-mode")
    } else if args.layout.is_some() {
        Some("--layout")
    } else if args.data_dir.is_some() {
        Some("--data-dir")
    } else if args.iterations != NonZeroU32::MIN {
        Some("--iterations")
    } else {
        None
    };
    match flag {
        Some(flag) => error::ModeBOnlyFlagSnafu { flag }.fail(),
        None => Ok(()),
    }
}

/// The cases that passed in the `baseline` report but not in `results`, with
/// their status here. A baseline that passed none of `results` compares
/// nothing, so it is an error rather than a clean comparison.
fn baseline_regressions(
    baseline: &Path,
    results: &[report::CaseResult],
) -> Result<Vec<(String, &'static str)>> {
    let text =
        std::fs::read_to_string(baseline).context(error::ReadFileSnafu { path: baseline })?;
    let report: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| baseline_error(baseline, &e.to_string()))?;
    let cases = report["results"]
        .as_array()
        .ok_or_else(|| baseline_error(baseline, "it has no `results` array"))?;
    let passed: std::collections::BTreeSet<&str> = cases
        .iter()
        .filter(|case| case["status"] == "passed")
        .filter_map(|case| case["test_id"].as_str())
        .collect();
    let compared: Vec<&report::CaseResult> = results
        .iter()
        .filter(|result| passed.contains(result.test_id.as_str()))
        .collect();
    if compared.is_empty() {
        return error::BaselineVacuousSnafu { path: baseline }.fail();
    }
    Ok(compared
        .into_iter()
        .filter(|result| result.status != report::TestStatus::Passed)
        .map(|result| (result.test_id.clone(), result.status.as_str()))
        .collect())
}

fn baseline_error(path: &Path, detail: &str) -> error::Error {
    error::BaselineSnafu {
        path: path.to_path_buf(),
        detail: detail.to_string(),
    }
    .build()
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

    /// `--spiced-path spiced` names a program the OS finds on `PATH`, as a
    /// shell would; a path with a directory in it is made absolute, since
    /// `spiced` runs in a temporary directory of its own.
    #[test]
    fn a_bare_spiced_name_is_left_for_the_path_lookup() {
        use std::path::{Path, PathBuf};
        let cwd = std::env::current_dir().expect("working directory");
        for (given, expected) in [
            ("spiced", PathBuf::from("spiced")),
            ("bin/spiced", cwd.join("bin/spiced")),
            ("../spiced", cwd.join("../spiced")),
            (
                "/usr/local/bin/spiced",
                PathBuf::from("/usr/local/bin/spiced"),
            ),
        ] {
            assert_eq!(
                super::spiced_program(Path::new(given)).expect(given),
                expected,
                "{given}"
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

    fn case(test_id: &str, status: crate::report::TestStatus) -> crate::report::CaseResult {
        crate::report::CaseResult {
            test_id: test_id.to_string(),
            description: String::new(),
            status,
            execution_time_ms: 0,
            error_message: None,
        }
    }

    fn baseline_file(statuses: &[(&str, &str)]) -> tempfile::NamedTempFile {
        let results: Vec<serde_json::Value> = statuses
            .iter()
            .map(|(test_id, status)| serde_json::json!({"test_id": test_id, "status": status}))
            .collect();
        let file = tempfile::NamedTempFile::new().expect("create the baseline file");
        std::fs::write(
            file.path(),
            serde_json::json!({ "results": results }).to_string(),
        )
        .expect("write the baseline file");
        file
    }

    /// A case that passed in the baseline and does not pass here is a
    /// regression; a case the baseline did not pass is not compared.
    #[test]
    fn a_case_that_passed_in_the_baseline_and_fails_here_is_a_regression() {
        use crate::report::TestStatus::{Error, Failed, Passed};
        let baseline = baseline_file(&[("q01", "passed"), ("q02", "passed"), ("q03", "failed")]);
        let results = [case("q01", Failed), case("q02", Passed), case("q03", Error)];
        let regressions = super::baseline_regressions(baseline.path(), &results)
            .expect("the baseline passed cases this run selected");
        assert_eq!(regressions, vec![("q01".to_string(), "failed")]);
    }

    /// A baseline that passed none of the selected cases would let any run
    /// through, so it must refuse rather than report no regressions.
    #[test]
    fn a_baseline_that_passed_none_of_the_selected_cases_is_refused() {
        use crate::report::TestStatus::Passed;
        let baseline = baseline_file(&[("q01", "error"), ("q02", "passed")]);
        let err = super::baseline_regressions(baseline.path(), &[case("q01", Passed)])
            .expect_err("a vacuous baseline");
        assert_eq!(
            err.to_string(),
            format!(
                "The baseline report '{}' passed none of the cases this run selected, so comparing \
                 against it would check nothing. Pass a baseline that passed them",
                baseline.path().display()
            )
        );
    }

    #[test]
    fn mode_a_refuses_a_mode_b_flag_it_would_ignore() {
        use clap::Parser as _;
        let args =
            super::Args::try_parse_from(["spice-substrait-compliance", "--layout", "primary_key"])
                .expect("clap accepts the layout");
        let err = super::reject_mode_b_flags(&args).expect_err("--layout with Mode A");
        assert_eq!(
            err.to_string(),
            "`--layout` applies to Mode B only, and Mode A would ignore it. Pass `--mode mode-b`, \
             or drop `--layout`"
        );
        let args = super::Args::try_parse_from(["spice-substrait-compliance", "--iterations", "1"])
            .expect("clap accepts one iteration");
        super::reject_mode_b_flags(&args).expect("one iteration is the Mode A default");
    }

    #[test]
    fn mode_a_refuses_an_acceleration_mode_it_would_ignore() {
        use clap::Parser as _;
        let args = super::Args::try_parse_from([
            "spice-substrait-compliance",
            "--acceleration-mode",
            "memory",
        ])
        .expect("clap accepts the mode");
        let err = super::reject_mode_b_flags(&args).expect_err("--acceleration-mode with Mode A");
        assert_eq!(
            err.to_string(),
            "`--acceleration-mode` applies to Mode B only, and Mode A would ignore it. Pass \
             `--mode mode-b`, or drop `--acceleration-mode`"
        );
    }

    /// A federated run has no acceleration, so a layout or a storage mode for
    /// one would be dropped and the run would test less than it was asked to.
    #[test]
    fn a_federated_run_refuses_a_flag_that_configures_an_acceleration() {
        use clap::Parser as _;
        let mode_b = |extra: &[&str]| {
            let mut argv = vec![
                "spice-substrait-compliance",
                "--mode",
                "mode-b",
                "--acceleration-engine",
            ];
            argv.extend_from_slice(extra);
            super::Args::try_parse_from(argv).expect("clap accepts the flags")
        };
        for (flag, value) in [("--layout", "primary_key"), ("--acceleration-mode", "file")] {
            let err = super::acceleration_options(&mode_b(&["none", flag, value]))
                .err()
                .unwrap_or_else(|| panic!("{flag} with --acceleration-engine none"));
            assert_eq!(
                err.to_string(),
                format!(
                    "`{flag}` configures an acceleration, and `--acceleration-engine none` \
                     serves the tables federated, without one. Pass an acceleration engine, or \
                     drop `{flag}`"
                )
            );
        }
        let federated =
            super::acceleration_options(&mode_b(&["none"])).expect("a plain federated run");
        assert!(federated.is_none(), "`none` serves the tables federated");

        let accelerated = super::acceleration_options(&mode_b(&[
            "cayenne",
            "--acceleration-mode",
            "memory",
            "--layout",
            "primary_key",
        ]))
        .expect("an accelerated run")
        .expect("an acceleration");
        assert_eq!(accelerated.engine, "cayenne");
        assert!(matches!(accelerated.mode, super::Mode_::Memory));
        assert_eq!(
            accelerated
                .layout
                .map(|layout| layout.to_string())
                .as_deref(),
            Some("primary_key")
        );
        let defaulted = super::acceleration_options(&mode_b(&["duckdb"]))
            .expect("an accelerated run")
            .expect("an acceleration");
        assert!(matches!(defaulted.mode, super::Mode_::File));
        assert!(defaulted.layout.is_none());
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
