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
//! (`datafusion-substrait` consumer). Mode B encodes `FlightSQL`
//! `CommandStatementSubstraitPlan` commands and skips execution until a
//! `spiced` fixture exists.

mod compare;
mod error;
mod mode_a;
mod mode_b;
mod report;
mod schema;
mod suite;

use std::path::PathBuf;
use std::process::ExitCode;

use chrono::Utc;
use clap::{Parser, ValueEnum};

use crate::error::Result;
use crate::report::ComplianceReport;
use crate::suite::load_tpch_suite;

/// IBM/substrait-compliance release this harness is pinned to.
pub const IBM_TAG: &str = "v0.1.1";

/// spiceai/datafusion git rev from the workspace `[patch.crates-io]`.
pub const DATAFUSION_FORK_REV: &str = "2e6ebfd97adcf6d6d192d1d4f23d2e67fff4395c";

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    /// `DataFusion` consumer baseline (IBM `examples/datafusion-rust` shape).
    #[value(name = "mode-a")]
    ModeA,
    /// `FlightSQL` `CommandStatementSubstraitPlan` stub (product path).
    #[value(name = "mode-b")]
    ModeB,
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

    /// Write the JSON report here.
    #[arg(
        long,
        default_value = "tools/substrait-compliance/results/mode-a-tpch.json"
    )]
    out_json: PathBuf,

    /// Write the per-query CSV here.
    #[arg(
        long,
        default_value = "tools/substrait-compliance/results/mode-a-tpch.csv"
    )]
    out_csv: PathBuf,

    /// `FlightSQL` endpoint used only by Mode B (not contacted yet).
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    flightsql_endpoint: String,
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
    let suite = load_tpch_suite(&args.suite)?;
    println!(
        "Loaded IBM suite '{}' v{} ({} cases) from {}",
        suite.name,
        suite.version,
        suite.cases.len(),
        suite.root.display()
    );
    if !suite.description.is_empty() {
        println!("{}", suite.description);
    }
    println!("IBM tag: {IBM_TAG}");
    println!("DataFusion fork rev: {DATAFUSION_FORK_REV}");

    let start = Utc::now();
    let (engine_name, engine_version, mode_name, results) = match args.mode {
        Mode::ModeA => {
            let data_dir = suite.root.join("data");
            let engine = mode_a::ModeAEngine::with_tpch_data(&data_dir).await?;
            let results = engine.run_suite(&suite, args.query.as_deref()).await?;
            (
                mode_a::ENGINE_NAME.to_string(),
                mode_a::ENGINE_VERSION.to_string(),
                "mode-a".to_string(),
                results,
            )
        }
        Mode::ModeB => {
            let engine = mode_b::FlightSqlComplianceEngine::new(&args.flightsql_endpoint);
            let cases: Vec<&_> = suite
                .cases
                .iter()
                .filter(|c| {
                    args.query
                        .as_deref()
                        .is_none_or(|q| c.id.eq_ignore_ascii_case(q))
                })
                .collect();
            for case in &cases {
                // Encode so a missing prost/FlightSQL type fails the stub itself.
                let _ = engine.run_case(case);
            }
            let owned: Vec<_> = cases.into_iter().cloned().collect();
            (
                mode_b::ENGINE_NAME.to_string(),
                mode_b::ENGINE_VERSION.to_string(),
                "mode-b".to_string(),
                mode_b::FlightSqlComplianceEngine::stub_results(&owned),
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
            ibm_tag: IBM_TAG.to_string(),
            datafusion_pin: format!("spiceai/datafusion@{DATAFUSION_FORK_REV}"),
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

    report.write_json(&args.out_json)?;
    report.write_csv(&args.out_csv)?;
    println!("Wrote {}", args.out_json.display());
    println!("Wrote {}", args.out_csv.display());

    // Report-only: never fail the process on a low pass rate. A non-zero
    // exit is reserved for harness I/O / load errors (already returned).
    Ok(ExitCode::SUCCESS)
}
